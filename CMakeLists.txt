cmake_minimum_required(VERSION 3.26)
project(Odyssey_lib CXX)

set(CMAKE_C_STANDARD 11)
add_subdirectory(btree)
include_directories(include)
include_directories(include/client_api)
include_directories(include/fifo)
include_directories(include/general_util)
include_directories(include/libhrd)
include_directories(include/mica)
include_directories(include/multicast)
include_directories(include/network_api)
include_directories(include/protocols)
include_directories(include/trace)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/btree)

add_executable(Odyssey_lib
        include/client_api/od_interface.h
        include/client_api/od_wrkr_side_calls.h
        include/fifo/od_fifo.h
        include/general_util/od_config_util.h
        include/general_util/od_debug_util.h
        include/general_util/od_generic_inline_util.h
        include/general_util/od_generic_macros.h
        include/general_util/od_generic_opcodes.h
        include/general_util/od_init_connect.h
        include/general_util/od_init_func.h
        include/general_util/od_inline_util.h
        include/general_util/od_latency_util.h
        include/general_util/od_rdma_gen_util.h
        include/general_util/od_sizes.h
        include/general_util/od_stats.h
        include/general_util/od_top.h
        include/libhrd/od_hrd.h
        include/mica/od_city.h
        include/mica/od_kvs.h
        include/multicast/od_multicast.h
        include/network_api/od_netw_func.h
        include/network_api/od_network_context.h
        include/network_api/od_templates.h
        include/protocols/od_kvs_prot_sel.h
        include/protocols/od_main_prot_sel.h
        include/protocols/od_stats_prot_sel.h
        include/protocols/od_top_prot_sel.h
        include/protocols/od_wrkr_prot_sel.h
        include/trace/od_trace_util.h
        src/client/od_client.c
        src/client/od_wrkr_side_calls.c
        src/client_api/od_interface.c
        src/libhrd/od_hrd_conn.c
        src/libhrd/od_hrd_util.c
        src/main/od_main.c
        src/main/od_stats.c
        src/main/od_worker.c
        src/mica/od_city.c
        src/mica/od_kvs.c
        src/multicast/od_multicast.c
        src/network_api/od_init_ctx.c
        src/network_api/od_init_qp_meta.c
        src/network_api/od_main_loop_api.c
        src/trace/od_trace.c
        ycsb.cpp)
target_link_libraries(src btree)
