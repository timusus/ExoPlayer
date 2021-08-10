/*
 * Copyright (C) 2020 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.android.exoplayer2.upstream;

import android.net.Uri;
import androidx.annotation.GuardedBy;
import androidx.annotation.Nullable;
import com.google.android.exoplayer2.C;
import com.google.android.exoplayer2.util.Assertions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
/**
 * A {@link DataSource} wrapper which allows to open a data source for peeking and also to keep a
 * connection open for future reuse.
 *
 * <p>The data source is first opened in peek mode. That means all read data is written to an
 * in-memory peek buffer. The peek position can also be reset with {@link #resetPeekPosition()}.
 * Calling {@link #stopPeeking()} while closed will exit the peek mode. All existing peek buffer
 * data will be reused if possible, but the data source will not save additional buffer in memory.
 *
 * <p>The underlying data source can be kept open for the next usage by calling {@link #keepOpen()}.
 * The caller must ensure to call {@link #closeIfKeptOpen()} to close the underlying source in case
 * it hasn't been used again.
 *
 * <p>Note that this data source is not thread-safe, except the call to {@link #closeIfKeptOpen()},
 * and must only be used on one thread at a time. It may be used on different threads over time, but
 * only if access happens strictly sequentially.
 */
public final class PeekDataSource implements DataSource {

  private static final int PEEK_BUFFER_INCREMENT = 256;

  private final DataSource dataSource;
  private final ArrayList<TransferListener> transferListeners;

  private boolean isPeeking;

  @Nullable private DataSpec peekDataSpec;
  private long peekSourceLength;
  private byte[] peekBuffer;
  private int peekBufferLength;
  private int peekBufferReadPosition;
  private boolean isClosed;
  @Nullable private DataSource transferListenerDataSource;
  @Nullable private DataSpec transferListenerDataSpec;
  private boolean transferListenerIsNetwork;

  // Access to this variable can happen on multiple threads because of the call to closeIfKeptOpen()
  @GuardedBy("this")
  private boolean isKeptOpen;

  /**
   * Creates a peeking data source.
   *
   * @param dataSource The upstream {@link DataSource}.
   */
  public PeekDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
    transferListeners = new ArrayList<>();
    isPeeking = true;
    peekBuffer = new byte[0];
    isClosed = true;
    dataSource.addTransferListener(new InternalTransferListener());
  }

  /** Resets the peek position to the start of the stream. Must only be called in peek mode. */
  public void resetPeekPosition() {
    Assertions.checkState(isPeeking);
    peekBufferReadPosition = 0;
  }

  /**
   * Stops peeking and enters normal (non-peeking) mode. Must only be called while the peek data
   * source is closed.
   */
  public void stopPeeking() {
    Assertions.checkState(isClosed);
    isPeeking = false;
  }

  /**
   * Keeps the data source open in the next call to {@link #close()} to allow future reuse. {@link
   * #closeIfKeptOpen()} must be called eventually to close the source in case it hasn't been used
   * again.
   */
  public synchronized void keepOpen() {
    Assertions.checkState(!isKeptOpen);
    Assertions.checkState(!isClosed);
    isKeptOpen = true;
  }

  /**
   * Closes the data source if it was kept open using {@link #keepOpen()} and hasn't been used
   * since. Otherwise, does nothing.
   *
   * @throws IOException If an {@link IOException} occurs while closing the data source.
   */
  public synchronized void closeIfKeptOpen() throws IOException {
    if (!isKeptOpen) {
      return;
    }
    isKeptOpen = false;
    closeInternal();
  }

  @Override
  public synchronized void addTransferListener(TransferListener transferListener) {
    if (!transferListeners.contains(transferListener)) {
      transferListeners.add(transferListener);
      if (isKeptOpen && transferListenerDataSource != null && transferListenerDataSpec != null) {
        transferListener.onTransferInitializing(
            transferListenerDataSource, transferListenerDataSpec, transferListenerIsNetwork);
        transferListener.onTransferStart(
            transferListenerDataSource, transferListenerDataSpec, transferListenerIsNetwork);
        transferListener.onBytesTransferred(
            transferListenerDataSource,
            transferListenerDataSpec,
            /* isNetwork= */ false,
            peekBufferLength);
      }
    }
  }

  @Override
  public synchronized long open(DataSpec dataSpec) throws IOException {
    isKeptOpen = false;
    isClosed = false;
    peekBufferReadPosition = 0;
    if (peekDataSpec != null) {
      if (peekDataSpec.isCompatibleWith(dataSpec)) {
        return peekSourceLength;
      } else {
        closeInternal();
      }
    }
    peekSourceLength = dataSource.open(dataSpec);
    peekDataSpec = dataSpec;
    return peekSourceLength;
  }

  @Override
  public int read(byte[] buffer, int offset, int readLength) throws IOException {
    if (isPeeking) {
      fillPeekBuffer(peekBufferReadPosition + readLength);
    }
    return peekBufferReadPosition < peekBufferLength
        ? readPeekBuffer(buffer, offset, readLength)
        : dataSource.read(buffer, offset, readLength);
  }

  @Nullable
  @Override
  public Uri getUri() {
    return isClosed ? null : dataSource.getUri();
  }

  @Override
  public Map<String, List<String>> getResponseHeaders() {
    return isClosed ? Collections.emptyMap() : dataSource.getResponseHeaders();
  }

  @Override
  public synchronized void close() throws IOException {
    isClosed = true;
    if (!isKeptOpen) {
      closeInternal();
    }
  }

  private void fillPeekBuffer(int length) throws IOException {
    if (length > peekBuffer.length) {
      int arrayLength = (length / PEEK_BUFFER_INCREMENT + 1) * PEEK_BUFFER_INCREMENT;
      peekBuffer = Arrays.copyOf(peekBuffer, arrayLength);
    }
    while (peekBufferLength < length) {
      int bytesRead = dataSource.read(peekBuffer, peekBufferLength, length - peekBufferLength);
      if (bytesRead == C.RESULT_END_OF_INPUT) {
        return;
      }
      peekBufferLength += bytesRead;
    }
  }

  private int readPeekBuffer(byte[] buffer, int offset, int readLength) {
    int peekBytes = Math.min(peekBufferLength - peekBufferReadPosition, readLength);
    System.arraycopy(peekBuffer, peekBufferReadPosition, buffer, offset, peekBytes);
    peekBufferReadPosition += peekBytes;
    return peekBytes;
  }

  private void closeInternal() throws IOException {
    peekDataSpec = null;
    peekBufferLength = 0;
    dataSource.close();
  }

  private final class InternalTransferListener implements TransferListener {

    @Override
    public void onTransferInitializing(DataSource source, DataSpec dataSpec, boolean isNetwork) {
      transferListenerDataSource = source;
      transferListenerDataSpec = dataSpec;
      transferListenerIsNetwork = isNetwork;
      for (int i = 0; i < transferListeners.size(); i++) {
        transferListeners.get(i).onTransferInitializing(source, dataSpec, isNetwork);
      }
    }

    @Override
    public void onTransferStart(DataSource source, DataSpec dataSpec, boolean isNetwork) {
      for (int i = 0; i < transferListeners.size(); i++) {
        transferListeners.get(i).onTransferStart(source, dataSpec, isNetwork);
      }
    }

    @Override
    public void onBytesTransferred(
        DataSource source, DataSpec dataSpec, boolean isNetwork, int bytesTransferred) {
      for (int i = 0; i < transferListeners.size(); i++) {
        transferListeners.get(i).onBytesTransferred(source, dataSpec, isNetwork, bytesTransferred);
      }
    }

    @Override
    public void onTransferEnd(DataSource source, DataSpec dataSpec, boolean isNetwork) {
      for (int i = 0; i < transferListeners.size(); i++) {
        transferListeners.get(i).onTransferEnd(source, dataSpec, isNetwork);
      }
    }
  }
}
