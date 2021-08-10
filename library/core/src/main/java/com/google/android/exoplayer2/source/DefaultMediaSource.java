package com.google.android.exoplayer2.source;

import static com.google.android.exoplayer2.util.Assertions.checkNotNull;

import androidx.annotation.Nullable;
import com.google.android.exoplayer2.C;
import com.google.android.exoplayer2.MediaItem;
import com.google.android.exoplayer2.Timeline;
import com.google.android.exoplayer2.drm.DrmSessionManager;
import com.google.android.exoplayer2.drm.DrmSessionManagerProvider;
import com.google.android.exoplayer2.metadata.icy.IcyHeaders;
import com.google.android.exoplayer2.offline.StreamKey;
import com.google.android.exoplayer2.source.MediaSourceEventListener.EventDispatcher;
import com.google.android.exoplayer2.upstream.Allocator;
import com.google.android.exoplayer2.upstream.DataSource;
import com.google.android.exoplayer2.upstream.DataSourceInputStream;
import com.google.android.exoplayer2.upstream.DataSpec;
import com.google.android.exoplayer2.upstream.DefaultLoadErrorHandlingPolicy;
import com.google.android.exoplayer2.upstream.HttpDataSource;
import com.google.android.exoplayer2.upstream.LoadErrorHandlingPolicy;
import com.google.android.exoplayer2.upstream.Loader;
import com.google.android.exoplayer2.upstream.Loader.LoadErrorAction;
import com.google.android.exoplayer2.upstream.Loader.Loadable;
import com.google.android.exoplayer2.upstream.PeekDataSource;
import com.google.android.exoplayer2.upstream.StatsDataSource;
import com.google.android.exoplayer2.upstream.TransferListener;
import com.google.android.exoplayer2.util.Assertions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * A {@link MediaSource} that supports multiple types of media. It automatically detects DASH, HLS
 * and SmoothStreaming media and falls back to progressive stream extraction otherwise.
 */
public final class DefaultMediaSource extends CompositeMediaSource<Void> {

  // LINT.IfChange
  @Nullable
  private static final Constructor<? extends MediaSourceFactory> DASH_FACTORY_CONSTRUCTOR =
      loadMediaSourceFactory("com.google.android.exoplayer2.source.dash.DashMediaSource$Factory");

  @Nullable
  private static final Constructor<? extends MediaSourceFactory> HLS_FACTORY_CONSTRUCTOR =
      loadMediaSourceFactory("com.google.android.exoplayer2.source.hls.HlsMediaSource$Factory");

  @Nullable
  private static final Constructor<? extends MediaSourceFactory> SS_FACTORY_CONSTRUCTOR =
      loadMediaSourceFactory(
          "com.google.android.exoplayer2.source.smoothstreaming.SsMediaSource$Factory");
  // LINT.ThenChange(../../../../../../../../proguard-rules.txt)

  /**
   * Exception thrown if the media isn't supported by any of the available {@link MediaSourceFactory
   * MediaSourceFactories}.
   */
  public static final class UnsupportedMediaException extends IOException {}

  /** Factory for {@link DefaultMediaSource}. */
  public static final class Factory implements MediaSourceFactory {

    private final ArrayList<MediaSourceFactory> mediaSourceFactories;

    private DataSource.Factory dataSourceFactory;
    @Nullable private LoadErrorHandlingPolicy loadErrorHandlingPolicy;
    @Nullable private List<StreamKey> streamKeys;
    @Nullable private DrmSessionManager drmSessionManager;

    /**
     * Creates a new factory for {@link DefaultMediaSource DefaultMediaSources}.
     *
     * @param dataSourceFactory A factory for {@link DataSource DataSources} to read the media.
     */
    public Factory(DataSource.Factory dataSourceFactory) {
      this.dataSourceFactory = dataSourceFactory;
      mediaSourceFactories = new ArrayList<>();
      if (HLS_FACTORY_CONSTRUCTOR != null) {
        mediaSourceFactories.add(
            createMediaSourceFactory(HLS_FACTORY_CONSTRUCTOR, dataSourceFactory));
      }
      if (DASH_FACTORY_CONSTRUCTOR != null) {
        mediaSourceFactories.add(
            createMediaSourceFactory(DASH_FACTORY_CONSTRUCTOR, dataSourceFactory));
      }
      if (SS_FACTORY_CONSTRUCTOR != null) {
        mediaSourceFactories.add(
            createMediaSourceFactory(SS_FACTORY_CONSTRUCTOR, dataSourceFactory));
      }
      mediaSourceFactories.add(new ProgressiveMediaSource.Factory(dataSourceFactory));
    }

    /**
     * Adds a {@link MediaSourceFactory} for creating {@link MediaSource} instances.
     *
     * <p>If a {@link MediaSourceFactory} of the same concrete class is already present, it will be
     * replaced.
     *
     * <p>The provided factory must only create {@link MediaSource#isSingleWindow() single-window
     * media sources}.
     *
     * @param mediaSourceFactory A {@link MediaSourceFactory} for creating {@link MediaSource}
     *     instances.
     * @return This factory, for convenience.
     */
    public Factory addMediaSourceFactory(MediaSourceFactory mediaSourceFactory) {
      Class<? extends MediaSourceFactory> clazz = mediaSourceFactory.getClass();
      for (int i = 0; i < mediaSourceFactories.size(); i++) {
        if (mediaSourceFactories.get(i).getClass().equals(clazz)) {
          mediaSourceFactories.set(i, mediaSourceFactory);
          return this;
        }
      }
      mediaSourceFactories.add(/* index= */ 0, mediaSourceFactory);
      return this;
    }

    @Override
    public Factory setDataSourceFactory(DataSource.Factory dataSourceFactory) {
      this.dataSourceFactory = dataSourceFactory;
      return this;
    }

    @Override
    public Factory setLoadErrorHandlingPolicy(
        @Nullable LoadErrorHandlingPolicy loadErrorHandlingPolicy) {
      this.loadErrorHandlingPolicy = loadErrorHandlingPolicy;
      return this;
    }

    @Override
    public Factory setStreamKeys(@Nullable List<StreamKey> streamKeys) {
      this.streamKeys = streamKeys;
      return this;
    }

    @Override
    public MediaSourceFactory setDrmSessionManagerProvider(
        @Nullable DrmSessionManagerProvider drmSessionManagerProvider) {
      for (int i = 0; i < mediaSourceFactories.size(); i++) {
        mediaSourceFactories.get(i).setDrmSessionManagerProvider(drmSessionManagerProvider);
      }
      return this;
    }

    @Override
    public Factory setDrmSessionManager(@Nullable DrmSessionManager drmSessionManager) {
      this.drmSessionManager = drmSessionManager;
      return this;
    }

    @Override
    public MediaSourceFactory setDrmHttpDataSourceFactory(
        @Nullable HttpDataSource.Factory drmHttpDataSourceFactory) {
      for (int i = 0; i < mediaSourceFactories.size(); i++) {
        mediaSourceFactories.get(i).setDrmHttpDataSourceFactory(drmHttpDataSourceFactory);
      }
      return this;
    }

    @Override
    public MediaSourceFactory setDrmUserAgent(@Nullable String userAgent) {
      for (int i = 0; i < mediaSourceFactories.size(); i++) {
        mediaSourceFactories.get(i).setDrmUserAgent(userAgent);
      }
      return this;
    }

    @Override
    public MediaSource createMediaSource(MediaItem mediaItem) {
      OverrideDataSourceFactory overrideDataSourceFactory =
          new OverrideDataSourceFactory(dataSourceFactory);
      ImmutableList.Builder<MediaSource> mediaSourceListBuilder = ImmutableList.builder();
      for (MediaSourceFactory factory : mediaSourceFactories) {
        factory.setDataSourceFactory(overrideDataSourceFactory);
        factory.setStreamKeys(streamKeys);
        factory.setDrmSessionManager(drmSessionManager);
        factory.setLoadErrorHandlingPolicy(loadErrorHandlingPolicy);
        MediaSource mediaSource = factory.createMediaSource(mediaItem);
        Assertions.checkState(mediaSource.isSingleWindow());
        mediaSourceListBuilder.add(mediaSource);
      }
      return new DefaultMediaSource(
          mediaItem,
          overrideDataSourceFactory,
          mediaSourceListBuilder.build(),
          loadErrorHandlingPolicy);
    }

    @Override
    @C.ContentType
    public int[] getSupportedTypes() {
      HashSet<Integer> supportedTypes = new HashSet<>();
      for (int i = 0; i < mediaSourceFactories.size(); i++) {
        for (int supportedType : mediaSourceFactories.get(i).getSupportedTypes()) {
          supportedTypes.add(supportedType);
        }
      }
      @C.ContentType int[] types = new int[supportedTypes.size()];
      int index = 0;
      for (Integer supportedType : supportedTypes) {
        types[index++] = supportedType;
      }
      return types;
    }
  }

  private final MediaItem mediaItem;
  private final MediaItem.PlaybackProperties playbackProperties;
  private final OverrideDataSourceFactory overrideDataSourceFactory;
  private final ImmutableList<MediaSource> candidateMediaSources;
  @Nullable private final LoadErrorHandlingPolicy loadErrorHandlingPolicy;
  private final PeekLoadableCallback peekLoadableCallback;
  private final EventDispatcher peekEventDispatcher;

  @Nullable private PeekDataSource peekDataSource;
  @Nullable private Loader peekLoader;
  @Nullable private MediaSource mediaSource;
  @Nullable private UnsupportedMediaException pendingUnsupportedMediaException;
  private boolean isPrepared;

  private DefaultMediaSource(
      MediaItem mediaItem,
      OverrideDataSourceFactory overrideDataSourceFactory,
      ImmutableList<MediaSource> candidateMediaSources,
      @Nullable LoadErrorHandlingPolicy loadErrorHandlingPolicy) {
    this.mediaItem = mediaItem;
    this.playbackProperties = checkNotNull(mediaItem.playbackProperties);
    this.overrideDataSourceFactory = overrideDataSourceFactory;
    this.candidateMediaSources = candidateMediaSources;
    this.loadErrorHandlingPolicy = loadErrorHandlingPolicy;
    peekLoadableCallback = new PeekLoadableCallback();
    peekEventDispatcher = createEventDispatcher(/* mediaPeriodId= */ null);
  }

  @Override
  public boolean canPrepareWithStream(InputStream inputStream) throws IOException {
    for (int i = 0; i < candidateMediaSources.size(); i++) {
      if (candidateMediaSources.get(i).canPrepareWithStream(inputStream)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void prepareSourceInternal(@Nullable TransferListener mediaTransferListener) {
    super.prepareSourceInternal(mediaTransferListener);
    isPrepared = false;
    if (mediaSource != null) {
      prepareChildSource(/* id= */ null, mediaSource);
      return;
    }
    peekDataSource = new PeekDataSource(overrideDataSourceFactory.createDataSource());
    peekLoader = new Loader("Loader:DefaultMediaSource");
    DataSpec dataSpec =
        new DataSpec.Builder()
            .setUri(playbackProperties.uri)
            .setFlags(DataSpec.FLAG_ALLOW_GZIP | DataSpec.FLAG_ALLOW_CACHE_FRAGMENTATION)
            .setHttpRequestHeaders(IcyHeaders.ICY_METADATA_REQUEST_HEADERS)
            .build();
    PeekLoadable loadable = new PeekLoadable(candidateMediaSources, peekDataSource, dataSpec);
    long elapsedRealtime =
        peekLoader.startLoading(loadable, peekLoadableCallback, getMinimumLoadableRetryCount());
    peekEventDispatcher.loadStarted(
        new LoadEventInfo(loadable.loadTaskId, dataSpec, elapsedRealtime), C.DATA_TYPE_UNKNOWN);
  }

  @Override
  public void maybeThrowSourceInfoRefreshError() throws IOException {
    super.maybeThrowSourceInfoRefreshError();
    if (peekLoader != null) {
      peekLoader.maybeThrowError();
    }
    if (pendingUnsupportedMediaException != null) {
      try {
        throw pendingUnsupportedMediaException;
      } finally {
        pendingUnsupportedMediaException = null;
      }
    }
  }

  @Override
  protected void onChildSourceInfoRefreshed(Void id, MediaSource mediaSource, Timeline timeline) {
    isPrepared = true;
    if (!isEnabled()) {
      closePeekDataSource();
    }
    refreshSourceInfo(timeline);
  }

  @Override
  public MediaItem getMediaItem() {
    return mediaItem;
  }

  @Override
  public void releaseSourceInternal() {
    super.releaseSourceInternal();
    pendingUnsupportedMediaException = null;
    releasePeekLoader();
    closePeekDataSource();
  }

  @Override
  public void disableInternal() {
    super.disableInternal();
    if (isPrepared) {
      closePeekDataSource();
    }
  }

  @Override
  public MediaPeriod createPeriod(MediaPeriodId id, Allocator allocator, long startPositionUs) {
    return checkNotNull(mediaSource).createPeriod(id, allocator, startPositionUs);
  }

  @Override
  public void releasePeriod(MediaPeriod mediaPeriod) {
    checkNotNull(mediaSource).releasePeriod(mediaPeriod);
  }

  private void onContentTypeDetected(int selectedMediaSourceFactoryIndex) {
    releasePeekLoader();
    checkNotNull(peekDataSource).stopPeeking();
    if (selectedMediaSourceFactoryIndex == C.INDEX_UNSET) {
      pendingUnsupportedMediaException = new UnsupportedMediaException();
      return;
    }
    mediaSource = candidateMediaSources.get(selectedMediaSourceFactoryIndex);
    overrideDataSourceFactory.setOverrideDataSource(peekDataSource);
    prepareChildSource(/* id= */ null, mediaSource);
  }

  private void releasePeekLoader() {
    if (peekLoader != null) {
      peekLoader.release();
      peekLoader = null;
    }
  }

  private void closePeekDataSource() {
    if (peekDataSource != null) {
      try {
        peekDataSource.closeIfKeptOpen();
      } catch (IOException e) {
        // Ignore.
      }
      overrideDataSourceFactory.setOverrideDataSource(null);
      peekDataSource = null;
    }
  }

  @Nullable
  private static Constructor<? extends MediaSourceFactory> loadMediaSourceFactory(
      String className) {
    try {
      Class<? extends MediaSourceFactory> factoryClazz =
          Class.forName(className).asSubclass(MediaSourceFactory.class);
      return factoryClazz.getConstructor(DataSource.Factory.class);
    } catch (Exception e) {
      // Expected if the app was built without the respective module.
      return null;
    }
  }

  private int getMinimumLoadableRetryCount() {
    return loadErrorHandlingPolicy == null
        ? DefaultLoadErrorHandlingPolicy.DEFAULT_MIN_LOADABLE_RETRY_COUNT
        : loadErrorHandlingPolicy.getMinimumLoadableRetryCount(C.DATA_TYPE_UNKNOWN);
  }

  private long getRetryDelayMs(LoadErrorHandlingPolicy.LoadErrorInfo loadErrorInfo,
      int errorCount) {
    return loadErrorHandlingPolicy == null
        ? Math.min((errorCount - 1) * 1000, 5000)
        : loadErrorHandlingPolicy.getRetryDelayMsFor(loadErrorInfo);
  }

  @Nullable
  private static MediaSourceFactory createMediaSourceFactory(
      @Nullable Constructor<? extends MediaSourceFactory> constructor,
      DataSource.Factory dataSourceFactory) {
    try {
      return constructor == null ? null : constructor.newInstance(dataSourceFactory);
    } catch (Exception e) {
      return null;
    }
  }

  private static final class PeekLoadable implements Loadable {

    public final long loadTaskId;

    private final List<MediaSource> candidateMediaSources;
    private final PeekDataSource peekDataSource;
    private final StatsDataSource dataSource;
    private final DataSpec dataSpec;

    private int selectedCandidateMediaSourceIndex;

    public PeekLoadable(
        List<MediaSource> candidateMediaSources, PeekDataSource dataSource, DataSpec dataSpec) {
      this.loadTaskId = LoadEventInfo.getNewId();
      this.candidateMediaSources = candidateMediaSources;
      this.peekDataSource = dataSource;
      this.dataSource = new StatsDataSource(peekDataSource);
      this.dataSpec = dataSpec;
      selectedCandidateMediaSourceIndex = C.INDEX_UNSET;
    }

    @Override
    public void cancelLoad() {
      // Ignore, because we only load a few bytes.
    }

    @Override
    public void load() throws IOException {
      dataSource.resetBytesRead();
      try (InputStream inputStream = new DataSourceInputStream(dataSource, dataSpec)) {
        for (int i = 0; i < candidateMediaSources.size(); i++) {
          MediaSource mediaSource = candidateMediaSources.get(i);
          if (mediaSource.canPrepareWithStream(inputStream)) {
            selectedCandidateMediaSourceIndex = i;
            peekDataSource.keepOpen();
            break;
          }
          peekDataSource.resetPeekPosition();
        }
      }
    }
  }

  private final class PeekLoadableCallback implements Loader.Callback<PeekLoadable> {

    @Override
    public void onLoadCompleted(
        PeekLoadable loadable, long elapsedRealtimeMs, long loadDurationMs) {
      peekEventDispatcher.loadCompleted(
          new LoadEventInfo(
              loadable.loadTaskId,
              loadable.dataSpec,
              loadable.dataSource.getLastOpenedUri(),
              loadable.dataSource.getLastResponseHeaders(),
              elapsedRealtimeMs,
              loadDurationMs,
              loadable.dataSource.getBytesRead()),
          C.DATA_TYPE_UNKNOWN);
      onContentTypeDetected(loadable.selectedCandidateMediaSourceIndex);
    }

    @Override
    public void onLoadCanceled(
        PeekLoadable loadable, long elapsedRealtimeMs, long loadDurationMs, boolean released) {
      peekEventDispatcher.loadCanceled(
          new LoadEventInfo(
              loadable.loadTaskId,
              loadable.dataSpec,
              loadable.dataSource.getLastOpenedUri(),
              loadable.dataSource.getLastResponseHeaders(),
              elapsedRealtimeMs,
              loadDurationMs,
              loadable.dataSource.getBytesRead()),
          C.DATA_TYPE_UNKNOWN);
    }

    @Override
    public LoadErrorAction onLoadError(
        PeekLoadable loadable,
        long elapsedRealtimeMs,
        long loadDurationMs,
        IOException error,
        int errorCount) {
      LoadEventInfo loadEventInfo = new LoadEventInfo(
          loadable.loadTaskId,
          loadable.dataSpec,
          loadable.dataSource.getLastOpenedUri(),
          loadable.dataSource.getLastResponseHeaders(),
          elapsedRealtimeMs,
          loadDurationMs,
          loadable.dataSource.getBytesRead());
      MediaLoadData mediaLoadData = new MediaLoadData(C.DATA_TYPE_UNKNOWN);
      long retryDelayMs = getRetryDelayMs(new LoadErrorHandlingPolicy.LoadErrorInfo(
          loadEventInfo, mediaLoadData, error, errorCount), errorCount);
      LoadErrorAction loadErrorAction =
          retryDelayMs == C.TIME_UNSET
              ? Loader.DONT_RETRY_FATAL
              : Loader.createRetryAction(/* resetErrorCount= */ false, retryDelayMs);
      peekEventDispatcher.loadError(
          loadEventInfo,
          C.DATA_TYPE_UNKNOWN,
          error,
          !loadErrorAction.isRetry());
      return loadErrorAction;
    }
  }

  private static final class OverrideDataSourceFactory implements DataSource.Factory {

    private final DataSource.Factory dataSourceFactory;

    @Nullable private DataSource overrideDataSource;

    public OverrideDataSourceFactory(DataSource.Factory dataSourceFactory) {
      this.dataSourceFactory = dataSourceFactory;
    }

    public void setOverrideDataSource(@Nullable DataSource dataSource) {
      overrideDataSource = dataSource;
    }

    @Override
    public DataSource createDataSource() {
      DataSource dataSource =
          overrideDataSource != null ? overrideDataSource : dataSourceFactory.createDataSource();
      overrideDataSource = null;
      return dataSource;
    }
  }
}
