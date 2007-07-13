import socket, select

from struct import pack, unpack

class MalformedMagic (Exception):
  pass

class Task (object):
  def __init__(self, func, argref, opts = dict()):

    assert type(argref) in (int, float, str, unicode), "Argref not a scalar"

    self.func = func
    self.argref = argref
    
    [setattr(self, x[0], x[1]) for x in opts.iteritems() if x[0] in ('uniq', 'on_complete', 'on_fail', 'on_retry', 'on_status', 'retry_count', 'timeout', 'high_priority')]
    
    self.retry_count = 0
    self.retries_done = 0
    self.is_finished = False

  def pack_submit_packet(self):
    pass

class Taskset (object):
  def add_task(self, func, args, opts):
    pass

class Packet (object):
  def __init__(self, type, len):
    self.type = type
    self.len = len
    self.data = ''
  
  def append_data(self, data):
    self.data += data
  
  def data_outstanding(self):
    return self.len - len(self.data)

class mysocket(object):
  def __init__(self, sock=None):
    # Create socket
    if sock is None:
      self.sock = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM)
    else:
      self.sock = sock
    self.header = ''

  def connect(self, host, port):
    self.sock.connect((host, port))

  def parse_data(self, data):
    print "parsing data: %r" % data
    while len(data):
      lendata = len(data)
      hdr_len = len(self.header)
      print "  bytes_remain = %d" % lendata
      print "  hdr_len      = %d" % hdr_len
      if hdr_len != 12:
        need = 12 - hdr_len
        self.header += data[:need]
        data = data[need:]
        if len(self.header) != 12:
          continue
        # we have a header, for sure, we have 12 bytes
        magic, _type, _len = unpack("!4sII", self.header)
        if magic != "\0RES":
          raise MalformedMagic
        self.pkt = Packet(_type, _len)
        continue
      
      # reading more data (past the header)
      need = self.pkt.data_outstanding()
      to_copy = max((need, lendata))
      self.pkt.append_data(data[:to_copy])
      data = data[to_copy:]
      if to_copy == need:
        self.on_packet(self.pkt, self)
        self.reset()
        
  def reset(self):
    self.header = ''
    self.pkt = None

########################################################################################################


HOST = '207.7.148.210'   
PORT = 19000

#create an INET, STREAMing socket
s = socket.socket(
    socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))

#X raw_input("DEBUG")

s.send('\0REQ\0\0\0\x07\0\0\0\x08foo\0\x001234567890')
s.setblocking(0)

def on_packet(pkt, ws):
  print "Got a packet..."
  print "%r" % pkt.data
  #exit(0)


ws = mysocket(s)
ws.on_packet = on_packet
#ws.wait()

while True:
  rr, blah, blah = select.select([ s ], [], [], 0)
  del blah
  for rs in rr:
    data = s.recv(1024)
    ws.parse_data(data)