/*
 * Copyright 2014–2018 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.concurrent

import java.lang.{Runnable, Runtime, Thread}
import java.util.concurrent.{Executors, ExecutorService, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.{math, StringContext}

import scalaz.concurrent.Strategy

object Pools {

  implicit val CPUExecutor: ExecutorService = {
    val numThreads = math.max(Runtime.getRuntime.availableProcessors, 4)
    Executors.newFixedThreadPool(numThreads, new ThreadFactory {
      private val counter = new AtomicInteger(0)

      def newThread(r: Runnable): Thread = {
        val t = new Thread(r)
        t.setName(s"quasar-cpu-thread-${counter.getAndIncrement}")
        t
      }
    })
  }

  implicit val CPUExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(CPUExecutor)

  implicit val CPUStrategy: Strategy = Strategy.Executor(CPUExecutor)
}
