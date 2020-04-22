import pymysql.cursors
from maga import Maga
from struct import pack,unpack
from time import sleep, time
import time
from queue import Queue
import asyncio
import threading
import socket
import signal
import requests
import json
import os
import base64
import bencoder
import math
from pybloom_live import ScalableBloomFilter, BloomFilter
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup as bs

es = Elasticsearch("localhost:9200")
connect = pymysql.Connect(
        host='localhost',
        port=3306,
        user='guest',
        passwd='xxx',
        db='dht_spider',
        charset='utf8')
cursor = connect.cursor()
BT_PROTOCOL = "BitTorrent protocol"
BT_MSG_ID = 20
EXT_HANDSHAKE = 0
infoQueue = Queue(maxsize=100000)
bloom = ScalableBloomFilter(initial_capacity=100, error_rate=0.001)

class Crawler(Maga):
    def random_node_id(self,size):
        return os.urandom(size) 
    def check_country(self,ip):  
        if ip:
            try:
                data = requests.get("https://sp0.baidu.com/8aQDcjqpAAV3otqbppnN2DJv/api.php?query="+ip+"&co=&resource_id=6006&t=1586779286008&ie=utf8&oe=gbk&cb=op_aladdin_callback&format=json&tn=baidu&cb=jQuery1102024254573672262314_1586779227570&_=1586779227662",timeout=5)
                if data.status_code == 200:
                    content = str(data.content,encoding= "gbk") 
                    if content.find('电信')+1 or content.find('联通')+1 or content.find('移动')+1 or content.find('香港')+1 or content.find('台湾')+1 or content.find('日本')+1 or content.find('韩国')+1:
                        print(content)
                        return True
                    return True
            except Exception as e:
                print(e)
                return False 
        return False     
     
    async def handle_announce_peer(self,infohash,addr,peer_addr):
        print(infohash)
        '''
        sql = "INSERT INTO torrent_info(infohash,addr) values('%s','%s','%s')"
        data = (infohash,addr[0],addr[1])
        cursor.execute(sql % data)
        connect.commit()
        '''
        if True:#self.check_country(peer_addr[0]):
            if infoQueue.qsize() <= 5000 and (infohash not in bloom):
                print('bloom ok')
                infoQueue.put((infohash,peer_addr))
                bloom.add(infohash)
            else:
                pass
    async def handle_get_peers(self,infohash,addr):
        #print(infohash)
        '''
        sql = "INSERT INTO torrent_info(infohash,addr) values('%s','%s','%s')"
        data = (infohash,addr[0],addr[1])
        cursor.execute(sql % data)
        connect.commit()
        '''
        #if self.check_country(addr[0]):
            #infoQueue.put((infohash,addr[0])) 
        if True:#self.check_country(peer_addr[0]):
            if infoQueue.qsize() <= 5000 and (infohash not in bloom):
                print('bloom ok')
                infoQueue.put((infohash,addr))
                bloom.add(infohash)
            else:
                pass
    def send_packet(self,_socket,msg):
        if type(msg) == bytes:
            _socket.send(msg)
        else:
            _socket.send(msg.encode())
        packet = _socket.recv(4069)
        return packet

    def send_ext_message(self,_socket,msg):
        msgLen = pack(">I",len(msg))
        msg = msgLen+msg
        packet = self.send_packet(_socket,msg)
        return packet

    def send_handshake(self,_socket,infohash):
        bt_header = chr(len(BT_PROTOCOL)) + BT_PROTOCOL
        bt_ext_byte = "\x00\x00\x00\x00\x00\x10\x00\x00"
        peerId = self.random_node_id(20)
        infohash = base64.b16decode(infohash)
        msg = bt_header.encode() + bt_ext_byte.encode() + infohash + peerId
        packet = self.send_packet(_socket,msg)
        return packet

    def check_handshake(self,packet,myHash):
        print('check handshake')
        try:
            bt_header_len, packet = ord(packet[:1]), packet[1:]
            if bt_header_len != len(BT_PROTOCOL):
                print('header len error')
                return False
        except TypeError:
            print('TypeError')
            return False
    
        bt_header, packet = packet[:bt_header_len], packet[bt_header_len:]
        if not bt_header.decode() == BT_PROTOCOL:
            print(bt_header)
            print(BT_PROTOCOL.encode())
            print('header error') 
            return False
    
        packet = packet[8:]
        infohash = packet[:20]
        myHash = base64.b16decode(myHash)
        
        if not infohash == myHash:
            print(infohash)
            print(myHash)
            print('infohash uncorrent')
            print(packet)
            return False
    
        return True
    def send_ext_handshake(self,_socket): 
        msg = chr(BT_MSG_ID).encode() + chr(EXT_HANDSHAKE).encode() + bencoder.bencode({"m":{"ut_metadata":1}}) 
        packet = self.send_ext_message(_socket,msg) 
        return packet
    def request_metadata(self,_socket,extHandShakeId,piece):
        msg = chr(BT_MSG_ID).encode() + chr(extHandShakeId).encode() + bencoder.bencode({"msg_type":0,"piece":piece})
        packet = self.send_and_recv_all(_socket,msg)
        return packet
        
    def parse_ut_metadata(self,data):
        metaData = "_metadata".encode()
        index = data.find(metaData) + len(metaData) + 1
        if index == -1:
            return 2
        return int(data[index])

    def parse_metadata_size(self,packet):
        _metadata_size = "metadata_size"
        size_index = packet.index(_metadata_size.encode())+1
        packet = packet[size_index:]
        ut_metadata_size = packet[len(_metadata_size):packet.index('e1'.encode())]
        #print(ut_metadata_size.decode())
        return int(ut_metadata_size.decode())     
       
        
    def send_and_recv_all(self,_socket,msg,timeout = 5):
        msgLen = pack(">I",len(msg))
        msg = msgLen+msg
        _socket.send(msg)
        total = b''
        while True: 
            try:
                data = _socket.recv(1024)
                if data:
                    total = total + data
            except socket.timeout as e:
                print(str(e))
                break
        return total

    def decode(data):
        result = ''
        for x in range('utf-8','gbk','GB2312','ascii'):
            try:
                result = data.decode(x)
            except Exception as e:
                pass
        return result
 
    def download_metadata(self,timeout=10):
        print('download-thread')
        while True:
            infohash,address = infoQueue.get() 
            sucess = self.request_magnet_page(infohash) 
            if sucess:
                continue
            else:
                sucess2 = self.request_magnet_page_2(infohash)
                if sucess2:
                    continue
            continue
            _socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
            metadata = b""
            try:
                _socket.settimeout(timeout)      
                _socket.connect((address[0],address[1]))
                packet = self.send_handshake(_socket,infohash)
                isCorrent = self.check_handshake(packet,infohash)
                if not isCorrent:
                    print(packet)
                    print('Uncorrent Handshake')
                    continue 
                packet = self.send_ext_handshake(_socket)
                ut_metadata = self.parse_ut_metadata(packet)
                metadata_size = self.parse_metadata_size(packet)
                for piece in range(int(math.ceil(metadata_size/(16*1024)))):
                    packet = self.request_metadata(_socket,2,piece)
                    index = packet.find('ee'.encode())
                    if index != -1:
                        packet = packet[index+2:]
                    metadata = metadata + packet
            except Exception as e:
                print('Error '+str(e)+' on :'+str(address[0])+":"+str(address[1]))
                pass
            finally:
                _socket.close()
            if metadata :
                print('Done '+infohash)
                info = self.parse_metadata(metadata)
                if info:
                    name = info["name"]
                    self.save_metadata(infohash,name,address,str(info))
    def parse_metadata(self, data): #解析种子
        info = {}
        self.encoding = 'utf8'
        try:
            torrent = bencoder.bdecode(data) #编码后解析
            
            if not torrent.get(b'name'):
                return None
        except:
            return None
        detail = torrent
        info['name'] = detail.get(b'name')
        if b'files' in detail:
            info['files'] = []
            for x in detail[b'files']:
                if b'path.utf-8' in x:
                    v = {'path': b'/'.join(x[b'path.utf-8']).decode('utf-8', 'ignore'), 'length': x[b'length']}
                else:
                    v = {'path': b'/'.join(x[b'path']).decode('utf-8','ignore'), 'length': x[b'length']}
                if b'filehash' in x:
                    v['filehash'] = base64.b16encode(x[b'filehash'])
                info['files'].append(v)
            info['length'] = sum([x['length'] for x in info['files']])
        else:
            info['length'] = detail[b'length']
        info['data_hash'] = base64.b16encode(detail[b'pieces']) #hashlib.md5(detail[b'pieces']).hexdigest()
        return info
    def save_metadata(self,infohash,name,address,info):
        sql = "insert into torrent_metadata(info_hash,address,name,info,magnet,update_time) values(%s,%s,%s,%s,%s,%s)"
        magnet = 'magnet:?xt=urn:btih:' + infohash
        updatetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        connect.ping(reconnect=True)
        cursor.execute(sql,(infohash,address[0],name,info,magnet,updatetime))
        connect.commit()
    def request_magnet_page_2(self,infohash): 
        url = 'https://btman.pw/zh-cn/magnet/'
        while True:
            targetUrl = url+str(infohash)
            try:
                response = requests.get(targetUrl,timeout=7)
                if response.status_code == 200:
                    html = response.text
                    soup = bs(html,'lxml')
                    name = soup.select('h2')[0].get_text()
                    print(name)
                    size = soup.select('.info-table td')[3].get_text()
                    count = 1
                    print(size)
                    magnet = 'magnet:?xt=urn:btih:'+infohash
                    updatetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    #data = {'name':name,'info_hash':infohash,'size':size,'magnet':magnet,'update_time':updatetime}
                    #es.index(index='torrent',doc_type='all_type',body=data,id=None) 
                    torrentsql = 'insert into torrent_metadata(name,info_hash,size,magnet,update_time) value(%s,%s,%s,%s,%s)' 
                    connect.ping(reconnect=True)
                    cursor.execute(torrentsql,(name,infohash,size,magnet,updatetime))
                    connect.commit()
                    print('Save :'+infohash)
                    return True
                else:
                    print('Not found :'+infohash)
                    return False             
            except Exception as e:
                print('Fail :'+str(e)+' in '+infohash)
                print('Start Peer wire...')
                return False
    def request_magnet_page(self,infohash):
        url = 'http://bq1.pw/magnet/'
        while True:
            targetUrl = url+str(infohash)
            try:
                response = requests.get(targetUrl,timeout=7)
                if response.status_code == 200:
                    html = response.text
                    soup = bs(html,'lxml')
                    name = soup.select('h5')[0].get_text()
                    print(name)
                    size = soup.select('.att-c')[1].get_text()
                    count = soup.select('.att-c')[2].get_text()
                    print(size)
                    files = []
                    for path in soup.select('.striped .path'):
                        files.append(path.get_text())
                    print(files)
                    if len(files) >1:
                        files = '/'.join(files)
                    else:
                        files = files[0]
                    magnet = 'magnet:?xt=urn:btih:'+infohash
                    updatetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
                    #data = {'name':name,'info_hash':infohash,'size':size,'magnet':magnet,'paths':files,'update_time':updatetime}
                    #es.index(index='torrent',doc_type='metadata',body=data,id=None)
                    torrentsql = 'insert into torrent_metadata(name,info_hash,size,magnet,paths,update_time) value(%s,%s,%s,%s,%s,%s)'
                    connect.ping(reconnect=True)
                    cursor.execute(torrentsql,(name,infohash,size,magnet,files,updatetime))
                    connect.commit()
                    print('Save :'+infohash)
                    return True
                else:
                    print('Not found :'+infohash)
                    return False
            except Exception as e:
                print('Fail :'+str(e)+' in '+infohash)
                print('Start Peer wire...')
                return False
    def run(self, port=6881): 
        
       
        coro = self.loop.create_datagram_endpoint(
                lambda: self, local_addr=('0.0.0.0', port)
        )
        transport, _ = self.loop.run_until_complete(coro)

        for signame in ('SIGINT', 'SIGTERM'):
            try:
                self.loop.add_signal_handler(getattr(signal, signame), self.stop)
            except NotImplementedError:
                # SIGINT and SIGTERM are not implemented on windows
                pass
        for i in range(20):
            threading.Thread(target=self.download_metadata).start()
        for node in self.bootstrap_nodes:
            # Bootstrap
            self.find_node(addr=node, node_id=self.node_id)
        asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
        self.loop.run_forever()
        self.loop.close()
crawler = Crawler()
crawler.run(6881)




