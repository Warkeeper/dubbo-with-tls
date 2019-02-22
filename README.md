# Dubbo with tls Project

## What is it  
This project aims at adding ssl/tls support to dubbo.  

## Why  
When business gets complicated,your system would be consist of many services and they may be deployed to hundreds of servers.Some servers may cross the outer net.  
Since providers and consumers communicates each other with plain text,it's necessary to encrypt the rpc content by ssl/tls.  

## How  
Since netty 4 has a good implement of ssl/tls,this project will focus on netty 4's api and implement ssl/tls support on dubbo-netty4-transport.  

## 中文介绍 （我的英文真是越来越差了呢）
这个项目是为了给dubbo增加ssl/tls支持，以保证服务之间调用时的通信信道是安全的。目前只支持netty4传输层。  
该项目目前正在进行，作为我个人的毕业设计，所以真的有人注意到了这个repo，并有任何建议，欢迎随时提出~

## 未来规划
1. 完成最核心的ssl/tls支持，通过spring配置，获取到root ca证书、服务端证书、服务端密钥（如果需要tls的客户端认证，那还需要客户端证书、客户端密钥），在传输层bind时增加ssl/tls handler。  
2. 为dubbo-admin增加ssl/tls相关页面功能，提供证书生成、签名功能
