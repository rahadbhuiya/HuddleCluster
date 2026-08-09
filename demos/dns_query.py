"""
Query the HuddleCluster DNS responder directly — a workaround for
Windows' built-in nslookup not supporting custom ports (-port flag is
Linux/BIND-nslookup only; Windows' nslookup always queries port 53).

Usage: python dns_query.py web.cluster.local
       python dns_query.py api.cluster.local
       python dns_query.py cache.cluster.local
"""
import socket
import struct
import sys

HOST = "127.0.0.1"
PORT = 8053


def build_query(name: str) -> bytes:
    txn_id  = b"\x12\x34"
    flags   = b"\x01\x00"   # standard recursive query
    qdcount = b"\x00\x01"
    rest    = b"\x00\x00\x00\x00\x00\x00"   # ancount/nscount/arcount = 0
    qname = b"".join(bytes([len(p)]) + p.encode() for p in name.split(".")) + b"\x00"
    qtype  = b"\x00\x01"   # A record
    qclass = b"\x00\x01"   # IN
    return txn_id + flags + qdcount + rest + qname + qtype + qclass


def parse_a_records(data: bytes, ancount: int):
    """Very small, purpose-built parser — good enough for this
    responder's own simple (non-compressed-pointer) answer format."""
    ips = []
    pos = 12
    # skip the question section: name + qtype(2) + qclass(2)
    while data[pos] != 0:
        pos += data[pos] + 1
    pos += 1 + 4
    for _ in range(ancount):
        # name (assume a pointer, 2 bytes) + type(2) + class(2) + ttl(4) + rdlength(2)
        pos += 2 + 2 + 2 + 4
        rdlength = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        if rdlength == 4:
            ip = ".".join(str(b) for b in data[pos:pos+4])
            ips.append(ip)
        pos += rdlength
    return ips


def main():
    name = sys.argv[1] if len(sys.argv) > 1 else "web.cluster.local"
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(3)
    print(f"Querying {name} at {HOST}:{PORT} ...")
    try:
        s.sendto(build_query(name), (HOST, PORT))
        data, _ = s.recvfrom(512)
    except socket.timeout:
        print(f"No response for '{name}'. Two possible reasons:")
        print(f"  1. run_service_discovery_demo.py isn't running (check port {PORT})")
        print( "  2. That service genuinely has no alive nodes / doesn't exist —")
        print( "     the responder silently drops queries with no answer")
        print( "     (by design: NXDOMAIN/NODATA both just get no reply,")
        print( "     same as a lot of minimal DNS responders do)")
        sys.exit(1)

    ancount = struct.unpack(">H", data[6:8])[0]
    if ancount == 0:
        print(f"{name} -> no records (service down or unknown)")
        return
    ips = parse_a_records(data, ancount)
    print(f"{name} -> {ips}")


if __name__ == "__main__":
    main()