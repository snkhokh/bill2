
def ip_ntos(n,prefix=32):
    tail = '/'+str(prefix) if prefix < 32 else ''
    return str(n>>24 & 255)+'.'+str(n>>16 & 255)+'.'+str(n>>8 &255)+'.'+str(n & 255) + tail

def ip_ston(addr):
    s = addr.split('/')
    lp = int(s.pop()) if len(s) > 1 else 32
    x = int(0)
    for i in s.split('.'):
       x <<= 8
       x += int(i)
    return x,lp
