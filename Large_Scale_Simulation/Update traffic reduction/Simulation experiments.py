#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import os
import random
import copy
import sys

#求两个区间的并集
def get_union(rangea, rangeb):
    rangea_lower, rangea_upper = rangea
    rangeb_lower, rangeb_upper = rangeb
    if rangea_upper + 1 == rangeb_lower:
        return [1, rangea_lower, rangeb_upper]
    if rangea_upper < rangeb_lower:
        return [2, rangea, rangeb]
    if rangea_lower <= rangeb_lower and rangea_upper >= rangeb_upper:
        return [1, rangea_lower, rangea_upper]
    elif rangea_lower <= rangeb_lower and rangea_upper < rangeb_upper:
        return [1, rangea_lower, rangeb_upper]

#求n个区间的并集
def get_nset_union(address_lists):
    section_lists = copy.deepcopy(address_lists)
    section_lists.sort(key=lambda range_item: range_item[0])
    i, j = 0, len(section_lists) - 1
    while True:
        if i == j:
            break
        rangea, rangeb = section_lists.pop(i),section_lists.pop(i)
        union_item = get_union(rangea, rangeb)
        if union_item[0] == 1:
            section_lists.insert(i,[union_item[1], union_item[2]])
        elif union_item[0] == 2:
            section_lists.insert(i, union_item[1])
            section_lists.insert(i + 1, union_item[2])
            i = i + 1
        j = len(section_lists) - 1
    return section_lists

def Raid_update(update_layout, m):
    traffic = 0
    for i in range(len(update_layout)):
        r = len(update_layout[i]) - m
        traffic = traffic + (r + m - 1)

    return traffic

def Delta_update(update_layout, update_address, block_size, m):
    traffic = 0

    for i in range(len(update_layout)):
        r = len(update_layout[i]) - m
        T = 0
        for j in range(r):
            for k in range(len(update_address[i][j])):
                T = T + ((update_address[i][j][k][1] - update_address[i][j][k][0] + 1) * 1.00 / block_size)
        traffic = traffic + T * m

    return traffic

# 返回两个地址的相交状态  -1:不相交  0：相交但不包含  1： 包含
def get_contain_state(address1, address2):
    state = -1
    if (address1[1] < address2[0]) or (address1[0] > address2[1]):
        state = -1
    elif (address1[0] <= address2[0] and address1[1] >= address2[1]) or (address1[0] >= address2[0] and address1[1] <= address2[1]):
        state = 1
    else:
        state = 0

    return state

def compute_solution_traffic(solution_list, update_address, m, block_size):
    traffic = 0
    for solution in solution_list:
        T = 0
        for i in range(len(solution)):
            T = T + ((solution[i][1] - solution[i][0] + 1) * 1.00 / block_size)
        union_address = get_nset_union(solution)
        if len(union_address) != 1:
            print("len(union_address) != 1")
            exit(-1)
        T = T + ((union_address[0][1] - union_address[0][0] + 1) * 1.00 / block_size) * (m - 1)
        traffic = traffic + T

    return traffic

def get_hybrid_traffic(update_stripe, update_address, m, block_size):
    upd_add = []
    for i in range(len(update_address)):
        for j in range(len(update_address[i])):
            upd_add.append(update_address[i][j])
    part_upd_num = len(upd_add)
    union_address_list = get_nset_union(upd_add)

    update_solution = []
    for union_address in union_address_list:
        solution = []
        for i in range(part_upd_num):
            if get_contain_state(union_address, upd_add[i]) == 1:
                solution.append(upd_add[i])
        update_solution.append(solution)
    traffic = compute_solution_traffic(update_solution, update_address, m, block_size)

    return traffic

def Hybrid_update(update_layout, update_address, block_size, m):
    traffic = 0

    for i in range(len(update_layout)):
        traffic = traffic + get_hybrid_traffic(update_layout[i], update_address[i], m, block_size)

    return traffic

def is_trigger_update(tri_upd_way, block_size, stripe_id, update_block_id, max_upd_stripe_num = 0, log_size = 0):
    if (tri_upd_way == "stripe" and max_upd_stripe_num <= 0) or (tri_upd_way == "log" and log_size <= 0):
        print("error tri_upd_way")
        exit(-1)

    cur_str_num = len(stripe_id)
    cur_log_size = len(update_block_id) * block_size * 1.00 / 1024  # MB

    if (tri_upd_way == "stripe" and cur_str_num >= max_upd_stripe_num) or (tri_upd_way == "log" and cur_log_size >= log_size):
        return True

    return False

def flip_address(block_address, block_size):
    flip_block_address = []

    for address in block_address:
        origin_start_add = address[0]
        origin_end_add = address[1]
        flip_block_address.append([block_size - origin_end_add, block_size - origin_start_add])

    return flip_block_address

def update_process(trace_file, trace_path, trace_type, block_size, k, m, upd_way, tri_upd_way, max_upd_stripe_num = 0, log_size = 0):
    block_size_B = block_size * 1024  # trace中是以B为单位
    update_block_id = []
    update_block_address = []  # 存储更新部分在块中的相对地址范围  [[[1,2], [3, 4]], [[2, 5]]]  一个块可能会有多个分开的更新的地址
    stripe_id = []
    traffic = 0
    name2size = get_Trace_info(trace_type)

    if trace_path.find('\\') != -1:
        filename = trace_path + '\\' + trace_file
    elif trace_path.find('/') != -1:
        filename = trace_path + '/' + trace_file
    else:
        print("cannot know OS")
        exit(-1)

    trace_name = trace_file[:-4]
    if name2size[trace_name] <= 1:
        request_num = 0
    elif name2size[trace_name] >= 200:
        request_num = 1000000
    else:
        request_num = get_rows(filename)

    total_upd_stri_num = 0 # 记录总的更新的条带数量
    count = 0
    with open(filename, encoding='utf-8') as fp:
        reader = csv.reader(fp)
        for Line in reader:
            count = count + 1
            if (trace_type == "MSR" and Line[3] == "Read") or (trace_type == "Ali" and Line[1] == "R") or (
                    trace_type == "Ten" and Line[3] == "0"):
                continue
            if trace_type == "MSR":
                offset = int(Line[-3])
                update_size = int(Line[-2])
            elif trace_type == "Ali":
                offset = int(Line[2])
                update_size = int(Line[3])
            elif trace_type == "Ten":
                offset = int(Line[1]) * 512
                update_size = int(Line[2]) * 512

            start_blk_id = int(offset / block_size_B)  # 集群中块id从0开始
            end_blk_id = int((offset + update_size) / block_size_B)

            for i in range(start_blk_id, end_blk_id + 1):
                address = []
                if i == start_blk_id and i != end_blk_id:
                    address.append(int(offset % block_size_B))
                    address.append(block_size_B)
                elif i == start_blk_id and i == end_blk_id:
                    address.append(int(offset % block_size_B))
                    address.append(int((offset + update_size) % block_size_B))
                elif i != start_blk_id and i != end_blk_id:
                    address.append(0)
                    address.append(block_size_B)
                elif i != start_blk_id and i == end_blk_id:
                    address.append(0)
                    address.append(int((offset + update_size) % block_size_B))
                if address[0] == 0 and address[1] == 0:
                    continue
                # 防止一个id重复出现
                if update_block_id.count(i) == 0:
                    update_block_address.append([[int(address[0] / 1024), int(address[1] / 1024)]])
                    update_block_id.append(i)
                else:
                    loc = update_block_id.index(i)
                    update_block_address[loc].append([int(address[0] / 1024), int(address[1] / 1024)])
                    update_block_address[loc] = get_nset_union(update_block_address[loc])


                id = int(i / k)  # 块所在的条带 id， 这里一定记得是除以数据块数量，而不是rs_n！！！
                if stripe_id.count(id) == 0:
                    stripe_id.append(id)
            blk_str_in_id = []  # 记录每个更新的条带中块在条带内的id，专用于flip
            if is_trigger_update(tri_upd_way, block_size, stripe_id, update_block_id, max_upd_stripe_num, log_size) == True or count + 1 >= request_num:
                stripe_id = []
                update_address = []  # [stripe1[block1[add1[]  add2[]]  block2[]]  stripe2[]  ]
                for i in range(len(update_block_id)):
                    id = int(update_block_id[i] / k)
                    in_id = update_block_id[i] % k  # 块在条带中的id (0 ~ k-1)，专用于flip
                    if stripe_id.count(id) == 0:
                        stripe_id.append(id)
                        update_address.append([update_block_address[i]])
                        blk_str_in_id.append([in_id])
                    else:
                        loc = stripe_id.index(id)
                        update_address[loc].append(update_block_address[i])
                        blk_str_in_id[loc].append(in_id)

                update_layout = []
                for i in range(len(update_address)):
                    r = len(update_address[i])
                    stripe = []
                    for j in range(r + m):
                        while (True):
                            node = random.randint(0, 100)
                            if stripe.count(node) == 0:
                                stripe.append(node)
                                break
                    update_layout.append(stripe)
                total_upd_stri_num = total_upd_stri_num + len(update_layout)
                if upd_way == "raid":
                    traffic = traffic + Raid_update(update_layout, m)
                elif upd_way == "delta":
                    traffic = traffic + Delta_update(update_layout, update_address, block_size, m)
                elif upd_way == "crd":
                    traffic = traffic + Hybrid_update(update_layout, update_address, block_size, m)
                elif upd_way == "flip":
                    for i in range(len(update_layout)):  # 遍历每个条带
                        for j in range(len(update_address[i])):  # 遍历每个条带中的块
                            if blk_str_in_id[i][j] % 2 == 1:
                                update_address[i][j] = flip_address(update_address[i][j], block_size)
                        traffic = traffic + get_hybrid_traffic(update_layout[i], update_address[i], m, block_size)
                else:
                    print("error way")
                    exit(-1)


                update_block_id = []
                update_block_address = []  # 存储更新部分在块中的相对地址范围
                stripe_id = []


            if count >= request_num:
                break

    return traffic, total_upd_stri_num

def get_Trace_info(type):
    if type == "MSR":
        path = "MSR-info.csv"
    elif type == "Ali":
        path = "Ali-info.csv"
    elif type == "Ten":
        path = "Ten-info.csv"

    name2size = {}
    with open(path, encoding='gb2312') as fp:
        reader = csv.reader(fp)
        for Line in reader:
            if Line[0] == "标识":
                continue
            name = Line[0]
            unit = Line[1][-2:]
            size = float(Line[1][:-2])
            if unit == "GB":
                size = size * 1024
            name2size[name] = size

    return name2size

def get_rows(tracefile):
    line = 0
    with open(tracefile, encoding='utf-8') as fp:
        reader = csv.reader(fp)
        for Line in reader:
            line = line + 1
    fp.close()

    return line

def obtain_have_cal_trace(filename):
    have_cal_trace = []
    try:
        with open(filename, "r", encoding='utf-8') as fp:
            reader = csv.reader(fp)
            for Line in reader:
                have_cal_trace.append(Line[0])
    except:
        have_cal_trace = []
    return have_cal_trace

if __name__ == '__main__':
    print("Update traffic reduction")
    k = 10
    m = 4
    n = 2000  # cluster node
    block_size = 64  # KB
    max_upd_stripe_num = 2
    log_size = 4 # MB
    tri_upd_way = "log"
    trace_type = "Ali"
    savefile = trace_type + "-traffic reduction.csv"

    trace_path = '../../trace/Ali'
    trace_file_list = os.listdir(trace_path)  # 得到文件夹下的所有文件名称

    unless_file = ['ID_zy.csv', 'nohup.out', 'rw.py', 'write_file.py', 'README.txt', 'VM_zy.csv', 'ID_491.csv']

    for trace_file in trace_file_list:
        have_cal_trave = obtain_have_cal_trace(savefile)
        if unless_file.count(trace_file) == 1 or have_cal_trave.count(trace_file) != 0:
            continue

        t_raid, str_num_raid = update_process(trace_file, trace_path, trace_type, block_size, k, m, "raid", tri_upd_way, max_upd_stripe_num, log_size)
        t_delta, str_num_delta = update_process(trace_file, trace_path, trace_type, block_size, k, m, "delta", tri_upd_way, max_upd_stripe_num, log_size)
        t_crd, str_num_crd = update_process(trace_file, trace_path, trace_type, block_size, k, m, "crd", tri_upd_way, max_upd_stripe_num, log_size)
        t_crd_f_s, str_num_crd_f_s = update_process(trace_file, trace_path, trace_type, block_size, k, m, "flip", tri_upd_way, max_upd_stripe_num, log_size)

        if t_raid == 0 or t_delta == 0 or t_crd == 0:
            continue

        data = [trace_file, t_raid, t_delta, t_crd, t_crd_f_s, str_num_crd_f_s]

        with open(savefile, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(data)
        csvfile.close()
        print(trace_file)