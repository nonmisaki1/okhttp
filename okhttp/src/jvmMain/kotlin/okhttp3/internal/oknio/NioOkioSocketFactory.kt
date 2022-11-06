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

import java.io.InterruptedIOException
import java.net.InetSocketAddress
import java.net.Proxy
import java.nio.channels.SelectionKey
import java.nio.channels.SelectionKey.OP_CONNECT
import java.nio.channels.SelectionKey.OP_READ
import java.nio.channels.SelectionKey.OP_WRITE
import java.nio.channels.Selector
import java.nio.channels.SocketChannel
import okhttp3.internal.concurrent.Task
import okhttp3.internal.concurrent.TaskRunner
import okhttp3.internal.notifyAll
import okhttp3.internal.wait
import okio.AsyncTimeout
import okio.Buffer
import okio.Sink
import okio.Source
import okio.buffer

class NioOkioSocketFactory(
  taskRunner: TaskRunner,
  internal val selector: Selector,
) : OkioSocket.Factory {
  private val selectorQueue = taskRunner.newQueue()

  private val task = object : Task("NIO Selector") {
    override fun runOnce(): Long {
      if (selector.keys().isEmpty()) return -1L

      selector.select()
      for (selectedKey in selector.selectedKeys()) {
        selectedKey.attachment().notifyAll()
      }

      return 0L
    }
  }

  fun enqueueSelect() {
    selector.wakeup()
    selectorQueue.schedule(task)
  }

  override fun create(proxy: Proxy, address: InetSocketAddress): OkioSocket {
    // TODO(jwilson): proxy.
    return SocketChannelOkioSocket(this, address)
  }
}

private class SocketChannelOkioSocket(
  val factory : NioOkioSocketFactory,
  private val address: InetSocketAddress,
) : OkioSocket {
  val monitor = Any()

  val channel: SocketChannel = SocketChannel.open()

  val key: SelectionKey = channel.register(factory.selector, 0, monitor)

  private val timeout = object : AsyncTimeout() {
    var triggered = false

    override fun timedOut() {
      synchronized(monitor) {
        triggered = true
        monitor.notifyAll()
      }
    }
  }

  override val connectTimeout = timeout

  override val source = SocketChannelSource(this).buffer()

  override val sink = SocketChannelSink(this).buffer()

  var canceled = false

  override fun connect() {
    synchronized(monitor) {
      connectTimeout.withTimeout {
        channel.configureBlocking(false)
        channel.connect(address)

        while (true) {
          channel.register(factory.selector, OP_CONNECT, monitor)
          factory.enqueueSelect()

          monitor.wait()
          key.interestOps(key.interestOps() and OP_CONNECT.inv())

          if (timeout.triggered) throw InterruptedIOException("timeout")
          if (canceled) throw InterruptedIOException("canceled")

          if (channel.finishConnect()) {
            return // Success.
          }
        }
      }
    }
  }

  override fun cancel() {
    synchronized(monitor) {
      canceled = true
      key.cancel()
      monitor.notifyAll()
    }
  }

  override fun close() {
    channel.close()
  }
}

private class SocketChannelSource(
  private var socket: SocketChannelOkioSocket,
) : Source {
  /** To batch up inbound reads. */
  private val buffer = Buffer()

  private val timeout = object : AsyncTimeout() {
    var triggered = false

    override fun timedOut() {
      val monitor = socket.monitor
      synchronized(monitor) {
        triggered = true
        monitor.notifyAll()
      }
    }
  }

  override fun timeout() = timeout

  override fun read(sink: Buffer, byteCount: Long): Long {
    refillBufferIfNecessary()
    return buffer.read(sink, byteCount)
  }

  private fun refillBufferIfNecessary() {
    val factory = socket.factory
    val selector = factory.selector
    val channel = socket.channel
    val key = socket.key
    val monitor = socket.monitor

    synchronized(monitor) {
      timeout.triggered = false
      timeout.withTimeout {
        while (buffer.exhausted()) {
          channel.register(selector, OP_READ, monitor)
          factory.enqueueSelect()

          monitor.wait()
          key.interestOps(key.interestOps() and OP_READ.inv())

          if (timeout.triggered) throw InterruptedIOException("timeout")
          if (socket.canceled) throw InterruptedIOException("canceled")

          if (key.isReadable) {
            channel.read(buffer)
          }
        }
      }
    }
  }

  override fun close() {
    buffer.clear() // Force unexpected reads to fail.
    socket.channel.close()
  }
}

private class SocketChannelSink(
  private val socket: SocketChannelOkioSocket,
) : Sink {
  /** To batch up outbound writes. */
  private val buffer = Buffer()

  private val timeout = object : AsyncTimeout() {
    var triggered = false

    override fun timedOut() {
      val monitor = socket.monitor
      synchronized(monitor) {
        triggered = true
        monitor.notifyAll()
      }
    }
  }

  override fun timeout() = timeout

  override fun write(source: Buffer, byteCount: Long) {
    buffer.write(source, byteCount)
    emit(all = false)
  }

  override fun flush() {
    emit(all = true)
  }

  fun emit(all: Boolean) {
    val factory = socket.factory
    val selector = factory.selector
    val channel = socket.channel
    val key = socket.key
    val monitor = socket.monitor

    // Defer system calls until we have a flush() or a full segment.
    if (!all && buffer.completeSegmentByteCount() == 0L) return

    synchronized(monitor) {
      timeout.triggered = false
      timeout.withTimeout {
        while (buffer.exhausted()) {
          channel.register(selector, OP_WRITE, monitor)
          factory.enqueueSelect()

          monitor.wait()
          key.interestOps(key.interestOps() and OP_WRITE.inv())

          if (timeout.triggered) throw InterruptedIOException("timeout")
          if (socket.canceled) throw InterruptedIOException("canceled")

          if (key.isWritable) {
            channel.write(buffer)
          }
        }
      }
    }
  }

  override fun close() {
    // Ensure resources don't leak.
    socket.channel.use {
      emit(all = true)
    }
  }
}
