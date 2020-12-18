package com.xyz.engine

/**
 * 全局唯一Id
 */
object GlobalGroupId {

  var groupId: Int = 0

  def getGroupId = {
    this.synchronized {
      groupId += 1
      groupId
    }
  }
}
