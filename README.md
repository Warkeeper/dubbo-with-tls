# Dubbo with tls Project

## What is it  
This project aims at adding ssl/tls support to dubbo.  

## Why  
When business gets complicated,your system would be consist of many services and they may be deployed to hundreds of servers.Some servers may cross the outer net.  
Since providers and consumers communicates each other with plain text,it's necessary to encrypt the rpc content by ssl/tls.  

## How  
Since netty 4 has a good implement of ssl/tls,this project will focus on netty 4's api and implement ssl/tls support on dubbo-netty4-transport.  
