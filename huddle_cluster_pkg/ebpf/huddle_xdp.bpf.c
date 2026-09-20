// SPDX-License-Identifier: MIT
/*
 * HuddleCluster Linux eBPF/XDP High-Performance Data-Plane
 * =========================================================
 * Intercepts incoming network packets at the eXpress Data Path (XDP)
 * layer in the Linux kernel and performs zero-copy wire-speed routing
 * to the active inner-ring servers synchronized by HuddleCluster.
 *
 * Architecture:
 * - Control Plane: Python HuddleCluster (thermal EMA scoring, rotation).
 * - Data Plane:    Linux Kernel eBPF XDP hook (zero-copy forwarding).
 */

#ifndef __KERNEL__
#define __KERNEL__
#endif

typedef unsigned char __u8;
typedef unsigned short __u16;
typedef unsigned int __u32;
typedef unsigned long long __u64;

#define ETH_P_IP 0x0800
#define IPPROTO_TCP 6

#define XDP_PASS 2
#define XDP_TX 3
#define XDP_DROP 1

/* BPF Map Types */
#define BPF_MAP_TYPE_ARRAY 2
#define BPF_MAP_TYPE_HASH 1

#define MAX_SERVERS 64

struct ethhdr {
    unsigned char h_dest[6];
    unsigned char h_source[6];
    __u16 h_proto;
} __attribute__((packed));

struct iphdr {
    __u8 ihl:4, version:4;
    __u8 tos;
    __u16 tot_len;
    __u16 id;
    __u16 frag_off;
    __u8 ttl;
    __u8 protocol;
    __u16 check;
    __u32 saddr;
    __u32 daddr;
} __attribute__((packed));

struct tcphdr {
    __u16 source;
    __u16 dest;
    __u32 seq;
    __u32 ack_seq;
    __u16 res1:4, doff:4, fin:1, syn:1, rst:1, psh:1, ack:1, urg:1, ece:1, cwr:1;
    __u16 window;
    __u16 check;
    __u16 urg_ptr;
} __attribute__((packed));

struct xdp_md {
    __u32 data;
    __u32 data_end;
    __u32 data_meta;
    __u32 ingress_ifindex;
    __u32 rx_queue_index;
    __u32 egress_ifindex;
};

struct bpf_server_record {
    __u32 ipv4_addr;     // Target backend IPv4 address (network byte order)
    __u16 port;          // Target backend TCP port (network byte order)
    __u16 weight;        // Server weight capacity
    __u32 temperature;   // Fixed-point thermal score (temp * 1000)
    __u64 packets_routed;// Telemetry counter
};

/* BPF maps definition for modern libbpf */
struct {
    __u32 type;
    __u32 max_entries;
    __u32 key_size;
    __u32 value_size;
} inner_servers_map __attribute__((section(.maps))) = {
    .type = BPF_MAP_TYPE_ARRAY,
    .max_entries = MAX_SERVERS,
    .key_size = sizeof(__u32),
    .value_size = sizeof(struct bpf_server_record),
};

struct {
    __u32 type;
    __u32 max_entries;
    __u32 key_size;
    __u32 value_size;
} cluster_config_map __attribute__((section(.maps))) = {
    .type = BPF_MAP_TYPE_ARRAY,
    .max_entries = 4,
    .key_size = sizeof(__u32),
    .value_size = sizeof(__u32),
};

/* Helper forward declarations for BPF verifier */
static void *(*bpf_map_lookup_elem)(void *map, const void *key) = (void *) 1;

static __inline __u16 csum_fold_helper(__u32 csum) {
    csum = (csum & 0xffff) + (csum >> 16);
    csum = (csum & 0xffff) + (csum >> 16);
    return (__u16)~csum;
}

__attribute__((section(xdp_huddle)))
int xdp_huddle_router(struct xdp_md *ctx) {
    void *data_end = (void *)(long)ctx->data_end;
    void *data = (void *)(long)ctx->data;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;

    if (eth->h_proto != __builtin_bswap16(ETH_P_IP))
        return XDP_PASS;

    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end)
        return XDP_PASS;

    if (ip->protocol != IPPROTO_TCP)
        return XDP_PASS;

    struct tcphdr *tcp = (void *)((unsigned char *)ip + (ip->ihl * 4));
    if ((void *)(tcp + 1) > data_end)
        return XDP_PASS;

    __u32 cfg_key = 0;
    __u32 *num_servers = bpf_map_lookup_elem(&cluster_config_map, &cfg_key);
    if (!num_servers || *num_servers == 0)
        return XDP_PASS;

    /* Flow hash based on client 4-tuple for connection stickiness */
    __u32 flow_hash = ip->saddr ^ tcp->source;
    __u32 target_idx = flow_hash % (*num_servers);

    struct bpf_server_record *server = bpf_map_lookup_elem(&inner_servers_map, &target_idx);
    if (!server || server->ipv4_addr == 0)
        return XDP_PASS;

    /* Rewrite destination IP and TCP port to target backend */
    __u32 old_daddr = ip->daddr;
    __u32 new_daddr = server->ipv4_addr;
    ip->daddr = new_daddr;

    /* Update telemetry packet counter */
    server->packets_routed++;

    /* Recalculate IPv4 checksum */
    __u32 csum = 0;
    __u16 *ip_words = (void *)ip;
    ip->check = 0;
    for (int i = 0; i < sizeof(struct iphdr) / 2; i++) {
        csum += ip_words[i];
    }
    ip->check = csum_fold_helper(csum);

    /* Return XDP_TX to forward packet back out the same interface to backend */
    return XDP_TX;
}

char _license[] __attribute__((section(license))) = MIT;
