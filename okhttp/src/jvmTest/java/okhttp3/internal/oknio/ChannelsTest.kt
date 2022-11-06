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
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ChannelsTest {
  @Test
  fun channelRead() {
    val sink = Buffer()
    val channel = Buffer().apply {
      writeUtf8("abcdefghij")
    }

    assertThat((channel as ReadableByteChannel).read(sink, 5L)).isEqualTo(5L)
    assertThat(sink.readUtf8()).isEqualTo("abcde")
    assertThat(channel.readUtf8()).isEqualTo("fghij")
  }

  @Test
  fun channelReadExhausted() {
    val sink = Buffer()
    val channel = Buffer()

    assertThat((channel as ReadableByteChannel).read(sink, 5L)).isEqualTo(-1L)
    assertThat(sink.size).isEqualTo(0L)
    assertThat(channel.size).isEqualTo(0L)
  }

  @Test
  fun channelReadSpanningSegments() {
    val a8192 = "a".repeat(8192)
    val b8192 = "b".repeat(8192)
    val c8192 = "c".repeat(8192)

    val sink = Buffer()
    val channel = Buffer().apply {
      writeUtf8(a8192)
      writeUtf8(b8192)
      writeUtf8(c8192)
      writeUtf8("defgh")
    }

    assertThat((channel as ReadableByteChannel).read(sink, 8192 * 3 + 2))
      .isEqualTo(8192 * 3 + 2)
    assertThat(sink.readUtf8()).isEqualTo(a8192 + b8192 + c8192 + "de")
    assertThat(channel.readUtf8()).isEqualTo("fgh")
  }

  @Test
  fun channelWrite() {
    val source = Buffer().apply {
      writeUtf8("abcdefghij")
    }
    val channel = Buffer()

    assertThat((channel as WritableByteChannel).write(source, 5L)).isEqualTo(5L)
    assertThat(channel.readUtf8()).isEqualTo("abcde")
    assertThat(source.readUtf8()).isEqualTo("fghij")
  }

  @Test
  fun channelWriteTargetExhausted() {
    val source = Buffer().apply {
      writeUtf8("abcde")
    }
    val channel = object : WritableByteChannel {
      override fun close() = Unit
      override fun isOpen(): Boolean = true
      override fun write(src: ByteBuffer?) = 0
    }
    assertThat((channel as WritableByteChannel).write(source, 5L)).isEqualTo(0L)
    assertThat(source.readUtf8()).isEqualTo("abcde")
  }

  @Test
  fun channelWriteSpanningSegments() {
    val a8192 = "a".repeat(8192)
    val b8192 = "b".repeat(8192)
    val c8192 = "c".repeat(8192)

    val source = Buffer().apply {
      writeUtf8(a8192)
      writeUtf8(b8192)
      writeUtf8(c8192)
      writeUtf8("defgh")
    }
    val channel = Buffer()

    assertThat((channel as WritableByteChannel).write(source, 8192 * 3 + 2))
      .isEqualTo(8192 * 3 + 2)
    assertThat(channel.readUtf8()).isEqualTo(a8192 + b8192 + c8192 + "de")
    assertThat(source.readUtf8()).isEqualTo("fgh")
  }
}
