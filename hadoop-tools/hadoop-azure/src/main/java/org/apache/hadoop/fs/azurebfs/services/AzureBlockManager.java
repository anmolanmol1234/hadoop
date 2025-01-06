/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.store.DataBlocks;

/**
 * Abstract base class for managing Azure Data Lake Storage (ADLS) blocks.
 */
public abstract class AzureBlockManager {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsOutputStream.class);

  /** Factory for blocks. */
  private final DataBlocks.BlockFactory blockFactory;

  /** Current data block. Null means none currently active. */
  protected AbfsBlock activeBlock;

  /** Count of blocks uploaded. */
  protected long blockCount = 0;

  /** The size of a single block. */
  protected final int blockSize;

  protected AbfsOutputStream abfsOutputStream;

  /**
   * Constructs an AzureBlockManager.
   *
   * @param abfsOutputStream the output stream associated with this block manager
   * @param blockFactory the factory to create blocks
   * @param blockSize the size of each block
   */
  protected AzureBlockManager(AbfsOutputStream abfsOutputStream,
      DataBlocks.BlockFactory blockFactory,
      final int blockSize) {
    this.abfsOutputStream = abfsOutputStream;
    this.blockFactory = blockFactory;
    this.blockSize = blockSize;
  }

  /**
   * Creates a new block at the given position.
   *
   * @param position the position in the output stream where the block should be created
   * @return the created block
   * @throws IOException if an I/O error occurs
   */
  protected final synchronized AbfsBlock createBlock(final long position)
      throws IOException {
    return createBlockInternal(position);
  }

  /**
   * Internal method to create a new block at the given position.
   *
   * @param position the position in the output stream where the block should be created.
   * @return the created block.
   * @throws IOException if an I/O error occurs.
   */
  protected abstract AbfsBlock createBlockInternal(final long position)
      throws IOException;

  /**
   * Gets the active block.
   *
   * @return the active block
   */
  protected synchronized AbfsBlock getActiveBlock() {
    return activeBlock;
  }

  /**
   * Checks if there is an active block.
   *
   * @return true if there is an active block, false otherwise
   */
  protected synchronized boolean hasActiveBlock() {
    return activeBlock != null;
  }

  /**
   * Gets the block factory.
   *
   * @return the block factory
   */
  protected DataBlocks.BlockFactory getBlockFactory() {
    return blockFactory;
  }

  /**
   * Gets the count of blocks uploaded.
   *
   * @return the block count
   */
  protected long getBlockCount() {
    return blockCount;
  }

  /**
   * Gets the block size.
   *
   * @return the block size
   */
  protected int getBlockSize() {
    return blockSize;
  }

  /**
   * Clears the active block.
   */
  void clearActiveBlock() {
    synchronized (this) {
      if (activeBlock != null) {
        LOG.debug("Clearing active block");
      }
      activeBlock = null;
    }
  }
}
