import os
import shutil
from protos.constants import *
from MapReduce.master import Master
import services_pb2


def user_input():
    input_dir = input("Enter input files directory: ")
    output_dir = input("Enter output files directory: ")
    mapper_num = int(input("Enter number of mappers: "))
    reducer_num = int(input("Enter number of reducers: "))
    return input_dir, mapper_num, reducer_num, output_dir


def refresh_storage():
    if os.path.isdir(INTERIM_DIR):
        shutil.rmtree(INTERIM_DIR)
    if os.path.isdir(PARTITION_DIR):
        shutil.rmtree(PARTITION_DIR)

    os.mkdir(INTERIM_DIR)
    os.mkdir(PARTITION_DIR)


def start_master_service(opt: int):
    refresh_storage()

    possible_feature = [
        services_pb2.FEATURE.WORD_COUNT,
        services_pb2.FEATURE.INVERTED_INDEX,
        services_pb2.FEATURE.NATURAL_JOIN,
    ]

    master = Master(
        input_dir, mapper_num, reducer_num, output_dir, possible_feature[opt - 1]
    )
    master.live()


if __name__ == "__main__":
    print("------------------- System Initialising -------------------")
    while True:
        print("\n\nSelect a Feature: ")
        print(" 1. Word Count")
        print(" 2. Inverted Index")
        print(" 3. Natural Join")
        print(" 4. EXIT\n: ", end="")

        opt = input()
        if opt == "1" or opt == "2" or opt == "3":
            input_dir, mapper_num, reducer_num, output_dir = user_input()
            start_master_service(int(opt))
        elif opt == "4":
            print("\n\nExiting...")
            break
        else:
            print("Error: invalid option")
