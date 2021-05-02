# -*- coding: utf-8 -*-
""" 
Created on Tue Apr 13 01:59:24 2021
@author: r.kalaivanan 
rule based method 
set theory is used 
"""

import numpy as np
import math 
from functools import reduce

def input_sudoku():
    global g_size
    global g_values
    global g_puzzle
    global g_rs # square root size  
    
    g_size = int(input('Enter size of the puzzle: '))
    g_rs = int(math.sqrt(g_size))
    print('Input the puzzle horizontally: ')
    g_values = np.array(list(set(range(1, g_size+1))))
    g_puzzle = np.zeros((g_size, g_size), dtype='i')
    print(g_puzzle)

    for i in range(g_size):
        for j in range(g_size):
            print('Enter value for ['+ str(i) + ']'+'['+ str(j) +']')
            while True:
              try:
                val = int(input())
                if val < 0 or val > g_size:
                    val = 0
                g_puzzle[i][j] = val
                break
              except ValueError:
                  print("Please input integer only")
                  continue

    print(g_puzzle)

    '''
    print(type(g_size))
    print(type(g_values))
    print(type(g_puzzle))
    print(type(g_rs))
    '''

def build_could_be_list():

    # this is the driving list
    # if this count is zero then puzzle solved 
    global could_be_list
    could_be_list = {}

    for i in range(g_size):
        for j in range(g_size):
            if g_puzzle[i][j] == 0:
                '''
                print(g_boxes[int((i//g_rs)*g_rs + j//g_rs)])
                print(g_rows[i])
                print(g_cols[j])
                print(np.union1d(np.union1d(g_boxes[(i//g_rs)*g_rs + j//g_rs], g_rows[i]), g_cols[j]))
                print(np.setdiff1d(g_values, np.union1d(np.union1d(g_boxes[(i//g_rs)*g_rs + j//g_rs], g_rows[i]), g_cols[j])))
                '''
                could_be_list[(i,j)] = np.setdiff1d(g_values, np.union1d(np.union1d(g_boxes[(i//g_rs)*g_rs + j//g_rs], g_rows[i]), g_cols[j]))
    # print(could_be_list)
    
def build_col_row_box():

    global g_cols
    global g_rows
    global g_boxes

    g_boxes = []
    g_rows = g_puzzle
    g_cols = g_puzzle.T
    for i in range(g_size):
         g_boxes.append([])
         g_boxes[i] = np.array(np.append([], [g_puzzle[j+((i//g_rs)*g_rs)][(i%g_rs)*g_rs:((i%g_rs)*g_rs)+g_rs] for j in range(g_rs)]), dtype='int64')

    g_boxes = np.array(g_boxes)

    '''
    print(type(g_boxes))
    print(type(g_cols))
    print(type(g_rows))
    print(type(g_values))
    print(g_size)
    print(g_rs)
    print(g_values)
    print(g_puzzle)
    print(g_rows)
    print(g_cols)
    print(g_boxes)
    '''
    
def fill_values(pos, val):
    print('filling', pos[0], pos[1], 'as', val)
    g_boxes[(pos[0]//g_rs)*g_rs + (pos[1]//g_rs)][(pos[0]%g_rs)*g_rs+pos[1]%g_rs] = val
    g_rows[pos[0]][pos[1]] = val
    g_puzzle[pos[0]][pos[1]] = val
    g_cols[pos[1]][pos[0]] = val
    could_be_list.pop(pos)

def apply_rules():
    # each rules applied one by one
    count = 0
    while (len(could_be_list) != 0): # could be list zero
        count += 1        

        # rule 1 : a point in could_be_list has only one element to be filled
        # it can be filled directly
        # before loop rebuild unilt there are not one elements
        while (True):
            fill_List = []
            fill_List = [eac for eac in could_be_list.keys() if len(could_be_list[eac]) == 1]
            if len(fill_List) == 0:
                print('nothing to fill.. 0 ')
                break
            for eac in fill_List:
                    fill_values(eac, could_be_list[eac])
            build_could_be_list()

        # rule 2 : its rebuilt in first rule
        # one element missing in a row
        while (True):
            for pos in could_be_list:
                # for row wise check columns
                col_nos         = []
                other_two_rows  = [[]]
                zero_count      = []
                fill_List       = {}

                col_nos = np.array(range(g_rs)) + (pos[1]//g_rs)*g_rs
                zero_count = [0 for eac in col_nos if g_puzzle[pos[0]][eac] == 0 and pos[1] != eac]
                if len(zero_count) == 0:
                    other_two_rows = [g_rows[i + (pos[0]//g_rs)*g_rs]  for i in range(g_rs) if pos[0]%g_rs != i]

                for eac in could_be_list[pos]:
                    if len(np.intersect1d(reduce(np.intersect1d, other_two_rows), eac)) == 1:
                        fill_List[pos] = eac
                        break 

            if len(fill_List) == 0:
                print('nothing to fill.. 1 ')
                break

            for eac in fill_List.keys():
                fill_values(eac, fill_List[eac])
            build_could_be_list()
            
        # one element missing in a column
        while (True):
            for pos in could_be_list:
                # for row wise check columns
                row_nos         = []
                other_two_cols  = [[]]
                zero_count      = []
                fill_List       = {}

                row_nos = np.array(range(g_rs)) + (pos[0]//g_rs)*g_rs
                zero_count = [0 for eac in row_nos if g_puzzle[eac][pos[1]] == 0 and pos[0] != eac]
                if len(zero_count) == 0:
                    other_two_cols = [g_cols[i + (pos[1]//g_rs)*g_rs]  for i in range(g_rs) if pos[1]%g_rs != i]

                for eac in could_be_list[pos]:
                    if len(np.intersect1d(reduce(np.intersect1d, other_two_cols), eac)) == 1:
                        fill_List[pos] = eac
                        break

            if len(fill_List) == 0:
                print('nothing to fill.. 2 ')
                break

            for eac in fill_List.keys():
                fill_values(eac, fill_List[eac])
            build_could_be_list()

        # rule 3: 
        
        if count >= g_size**2:
            break

# driver function
def solve_sudoku():
    input_sudoku()
    build_col_row_box()
    build_could_be_list()
    apply_rules()
    print(g_puzzle)
    print('solved!!')
