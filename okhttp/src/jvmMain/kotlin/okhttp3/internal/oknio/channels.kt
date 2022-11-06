/*
 * Copyright (C) 2022 Block, Inc.
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
package okhttp3.internal.oknio

import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel
import okio.Buffer

/**
 * Reads up to [byteCount] bytes from this into [sink].
 *
 * This will read fewer bytes if this channel is exhausted.
 *
 * It will also read fewer bytes if this channel is in non-blocking mode, and reading more bytes
 * would require blocking.
 *
 * @return the number of bytes read, possibly 0. Returns -1 if zero bytes were read and this channel
 *   is exhausted.
 */
fun ReadableByteChannel.read(sink: Buffer, byteCount: Long = Long.MAX_VALUE): Long {
  var result = 0L
  val cursor = Buffer.UnsafeCursor()
  while (result < byteCount) {
    sink.readAndWriteUnsafe(cursor).use {
      val oldSize = sink.size
      val length = minOf(byteCount - result, 8192).toInt()
      cursor.expandBuffer(length)
      val readResult = read(ByteBuffer.wrap(cursor.data, cursor.start, length))
      if (readResult <= 0L) {
        cursor.resizeBuffer(oldSize)
        return@read when (result) {
          0L -> -1L
          else -> result
        }
      }

      result += readResult
      cursor.resizeBuffer(oldSize + readResult)
    }
  }

  return result
}

/**
 * Writes up to [byteCount] bytes from [source] into into this.
 *
 * This will write fewer bytes if this channel is ready to accept new data.  This typically occurs
 * when this channel is in blocking mode and writing more bytes would require blocking.
 *
 * @return the number of bytes written, possibly 0.
 */
fun WritableByteChannel.write(source: Buffer, byteCount: Long = source.size): Long {
  var result = 0L
  val cursor = Buffer.UnsafeCursor()
  while (result < byteCount) {
    source.readUnsafe(cursor).use {
      cursor.seek(0L)
      val length = minOf(byteCount - result, (cursor.end - cursor.start).toLong())
      val writeResult = write(ByteBuffer.wrap(cursor.data, cursor.start, length.toInt()))
      if (writeResult == 0) return@write result

      result += writeResult
      source.skip(writeResult.toLong())
    }
  }

  return result
}
