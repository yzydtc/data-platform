package com.xyz.common;

import com.xyz.domain.engine;
import com.xyz.utils.ZKUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class AkkaUtils {

  public static Config getConfig(ZkClient zkClient) {
    //初始化ip
    String ip = "localhost";
    //初始化port
    Integer port = 3000;
    //初始化引擎id
    Integer id = 1;
    try {
      ip = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

    final Seq<engine.PlatEngine> platEngineInCluster = ZKUtils.getPlatEngineInCluster(zkClient);
    //保证id的唯一，先获取id，然后自增
    List engineidList = new ArrayList<Integer>();
    List enginePortList = new ArrayList<Integer>();
    //获取迭代器
    Iterator<engine.PlatEngine> engineIterator = platEngineInCluster.iterator();
    //循环获取数据
    while (engineIterator.hasNext()) {
      engine.PlatEngine engine = engineIterator.next();
      engineidList.add(engine.engineId());
      if (engine.engineInfo().contains(ip)) {
        enginePortList.add(Integer.parseInt(engine.engineInfo().split(":")[1]));
      }
    }

    //id顺序增加
    while (engineidList.size() != 0) {
      while (engineidList.contains(id)) {
        id += 1;
      }
    }
    //端口顺序增加
    while (enginePortList.size() != 0) {
      while (enginePortList.contains(port)) {
        port += 1;
      }
    }

    //注册引擎信息到zk
    ZKUtils.registerEngineInZookeeper(zkClient, id, ip, port);
    //封装akka信息
    Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port)
        .withFallback(
            ConfigFactory.parseString("akka.actor.provider=akka.remote.RemoteActorRefProvider"))
        .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + ip))
        .withFallback(ConfigFactory.load());
    return config;
  }
}
