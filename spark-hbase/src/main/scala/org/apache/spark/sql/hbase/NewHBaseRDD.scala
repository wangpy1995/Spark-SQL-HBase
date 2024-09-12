/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hbase

import org.apache.hadoop.classification.InterfaceAudience
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.annotation.meta.param

@InterfaceAudience.Public
class NewHBaseRDD[K, V](@(transient@param) sc: SparkContext,
                        @(transient@param) inputFormatClass: Class[_ <: InputFormat[K, V]],
                        @(transient@param) keyClass: Class[K],
                        @(transient@param) valueClass: Class[V],
                        @transient private val _conf: Configuration,
                        val hBaseContext: HBaseSQLContext) extends NewHadoopRDD(sc, inputFormatClass, keyClass, valueClass, _conf) {
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val jconf = new JobConf(getConf)
    SparkHadoopUtil.get.addCredentials(jconf)
    super.compute(theSplit, context)
  }
}

object NewHBaseRDD {
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()
}
