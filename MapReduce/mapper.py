import services_pb2 as services_pb2
import services_pb2_grpc as services_pb2_grpc
from google.protobuf import empty_pb2

import hashlib


class Mapper(services_pb2_grpc.MapperReducerServicer):
    def __init__(self, address, port) -> None:
        print("------------------- Mapper Initialise -------------------\n")
        print(f"[+] Mapper #{port-50000} is Live at {address}:{port}\n")

        self.worker_status = services_pb2.STATUS.IDLE

        self.curr_working_file = None
        self.table_heading = None

    ######################### RPC FUNCTIONS ###########################

    def get_worker_status(self, request, context):
        return services_pb2.WORKER_STATUS(status=self.worker_status)

    def set_worker_status(self, request, context):
        self.worker_status = request.status
        return empty_pb2.Empty()

    def assign_worker_a_task(self, request, context):
        feature = request.feature
        task = request.task
        in_file = request.input_file
        out_file = request.output_file
        out_dir_files_num = request.output_dir_files_num

        self.curr_working_file = in_file.strip().split("/")[-1]

        feature_fun = {
            services_pb2.FEATURE.WORD_COUNT: self.word_count,
            services_pb2.FEATURE.INVERTED_INDEX: self.inverted_index,
            services_pb2.FEATURE.NATURAL_JOIN: self.natural_join,
        }

        print("\n[-] Assigned 'A NEW TASK': ")
        if task == services_pb2.STATUS.INTERIM_START:
            self.input_to_intermediate(feature_fun[feature], in_file, out_file)
        elif task == services_pb2.STATUS.PARTITION_START:
            self.intermediate_to_partition(in_file, out_file, out_dir_files_num)
        return empty_pb2.Empty()

    ######################### General FUNCTIONS ###########################

    def word_count(self, key_value: dict, ith_line: int, line: str):
        for word in line.strip().split():
            if word in key_value.keys():
                key_value[word] += 1
            else:
                key_value[word] = 1

    def inverted_index(self, key_value: dict, ith_line: int, line: str):
        for word in line.strip().split():
            key_value[word] = self.curr_working_file

    def natural_join(self, key_value: dict, ith_line: int, line: str):
        word1, word2 = line.strip().split(",")
        word1 = word1.strip()
        word2 = word2.strip()

        if word1 == "Name" and (word2 == "Age" or word2 == "Role"):
            self.table_heading = (word1, word2)
            return

        if word1 in key_value.keys():
            new_pair = f"({self.table_heading[1]}@{word2})"
            new_value = f"{key_value[word1]};{new_pair}"
            key_value[word1] = new_value
        else:
            key_value[word1] = f"({self.table_heading[1]}@{word2})"

    def input_to_intermediate(self, feature_fun, in_file: str, out_file: str):
        print(f"[+] Performing Input -> Intermediate Task on file: {in_file}")
        self.worker_status = services_pb2.STATUS.INTERIM_WORKING

        key_value = dict()
        f1 = open(in_file, "r")
        for i, line in enumerate(f1.readlines()):
            if not len(line):
                continue

            # calling feature function
            feature_fun(key_value, i, line)
        f1.close()

        f2 = open(out_file, "w")
        for key, val in key_value.items():
            f2.write(f"({key},{val})\n")
        f2.close()

        print(
            f"[+] Done Input -> Intermediate Task on file: {in_file}\t to file: {out_file}"
        )
        self.worker_status = services_pb2.STATUS.INTERIM_DONE

    def intermediate_to_partition(self, in_file, out_dir, out_dir_files_num):
        print(f"[+] Performing Intermediate -> Partition Task on file: {in_file}")
        self.worker_status = services_pb2.STATUS.PARTITION_WORKING

        f1 = open(in_file, "r")
        for line in f1.readlines():
            if not len(line):
                continue

            key, value = line.strip()[1:-1].split(",")
            partition_num = (
                int(hashlib.sha256(key.encode()).hexdigest(), 16)
            ) % out_dir_files_num

            path = f"{out_dir}/partition_{partition_num + 1}.txt"

            f2 = open(path, "a")
            f2.write(line)
            f2.close()

        f1.close()
        print(f"[+] Done Input -> Intermediate Task on file: {in_file}")
        self.worker_status = services_pb2.STATUS.PARTITION_DONE
