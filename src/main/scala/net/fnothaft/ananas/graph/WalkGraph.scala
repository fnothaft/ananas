/**
 * Copyright 2015 Frank Austin Nothaft
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
package net.fnothaft.ananas.graph

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object WalkGraph extends Serializable with Logging {

  def apply[VD, ED, MT, ET: ClassTag](graph: Graph[VD, ED],
                                      sendInitialMessages: (VertexId, VD) => Iterable[(VertexId, MT)],
                                      walkFn: (Iterable[(VertexId, MT)], VertexId, VD) => (VD, Iterable[(VertexId, MT)], Iterable[MT]),
                                      emitFn: MT => ET): RDD[ET] = {

    // map the graph to add "complete message bucket" and cache it
    var bucketGraph = graph.mapVertices((vid, vdata) => (vdata, Iterable.empty[MT]))
      .cache()

    // initialize the messages
    var msgs: VertexRDD[Iterable[(VertexId, MT)]] = bucketGraph.vertices
      .aggregateUsingIndex(bucketGraph.vertices
      .flatMap(vtx => {
        val (vid, (v, _)) = vtx
        sendInitialMessages(vid, v).map(kv => (kv._1, Iterable((vid, kv._2))))
      }), (i1: Iterable[(VertexId, MT)], i2: Iterable[(VertexId, MT)]) => i1 ++ i2)
        .cache()

    // how many nodes are being walked in this iteration?
    var walkedNodeCount = msgs.count()
    var iter = 1

    log.info("Started walks from %d nodes.".format(walkedNodeCount))

    // loop
    while(walkedNodeCount > 0) {
      log.info("Walking across %d nodes in iteration %d.".format(walkedNodeCount,
                                                                 iter))

      // update the graph
      val messageGraph = bucketGraph.outerJoinVertices(msgs)((vid, vkv, ot) => {
                                                        val (vd, deadMsgs) = vkv
                                                        
                                                        (vd, ot, deadMsgs)
                                                      }).mapVertices((vid, vdata) => {
        val (vd, optionalReceivedMsgs, deadMsgs) = vdata

        optionalReceivedMsgs.fold((vd, Iterable.empty[(VertexId, MT)], deadMsgs))(receivedMsgs => {
          val (newVd, sendMsgs, newDeadMsgs) = walkFn(receivedMsgs, vid, vd)
                                                         
          require(sendMsgs.size + newDeadMsgs.size == receivedMsgs.size,
                  "At vertex %l, we received %d messages, but sent %d and retired %d messages.".format(
            receivedMsgs.size,
            sendMsgs.size,
            newDeadMsgs.size))
          
          (newVd, sendMsgs, deadMsgs ++ newDeadMsgs)
        })
      }).cache()

      // discard old bucket graph and messages
      bucketGraph.unpersist(blocking = false)
      msgs.unpersist()

      // aggregate new messages together
      msgs = messageGraph.vertices.aggregateUsingIndex(messageGraph.vertices
      .flatMap(vtx => {
        vtx._2._2.map(kv => (kv._1, Iterable((vtx._1, kv._2))))
      }), (i1: Iterable[(VertexId, MT)], i2: Iterable[(VertexId, MT)]) => i1 ++ i2)
        .cache()

      // discard sent messages from graph
      bucketGraph = messageGraph.mapVertices((vid, vdata) => {
        val (vd, _, deadMsgs) = vdata
        (vd, deadMsgs)
      }).cache()

      // unpersist message graph
      messageGraph.unpersist(blocking = false)

      walkedNodeCount = msgs.count()
      iter += 1
    }

    log.info("Completed walks in %d iterations.".format(iter - 1))

    // emit dead messages
    val emittedRdd = bucketGraph.vertices
      .flatMap(vtx => {
        vtx._2._2.map(emitFn)
      })

    // unpersist final bucket graph
    bucketGraph.unpersist(blocking = false)

    emittedRdd
  }
}
