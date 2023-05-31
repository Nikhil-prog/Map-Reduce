import grpc
import time
import os
import services_pb2 as services_pb2
import services_pb2_grpc as services_pb2_grpc
from google.protobuf import empty_pb2
from protos.constants import *
import math
import random


class Master:
    def __init__(self, input_dir, mapper_num, reducer_num, output_dir, feature) -> None:
        self.input_dir = input_dir
        self.mapper_num = mapper_num
        self.reducer_num = reducer_num
        self.output_dir = output_dir

        self.feature = feature

        # Mapper stub
        mapper_port = 50001
        self.mapper_stubs = dict()
        for _ in range(0, self.mapper_num):
            mapper_id = _ + 1
            self.mapper_stubs[mapper_id] = self.create_stubs("localhost", mapper_port)
            mapper_port += 1

        # Reducer stub
        reducer_port = 50101
        self.reducer_stubs = dict()
        for _ in range(0, self.mapper_num):
            reducer_id = _ + 1
            self.reducer_stubs[reducer_id] = self.create_stubs(
                "localhost", reducer_port
            )
            reducer_port += 1

    def live(self):
        print("------------------- Feature Initialising -------------------")

        # assigning input data -> intermediate data
        time.sleep(2)
        self.handle_communication(
            "Mapper",
            self.input_dir,
            self.mapper_stubs,
            services_pb2.STATUS.INTERIM_START,
            services_pb2.STATUS.INTERIM_DONE,
            "Assigning Intermediate Data creation task to mappers...",
            "Intermediate Data creation succesfull by all mappers",
            len(self.get_all_files_from_a_dir(self.input_dir)),
            None,
        )

        #  assigning intermediate data -> partition
        time.sleep(2)
        self.handle_communication(
            "Mapper",
            INTERIM_DIR,
            self.mapper_stubs,
            services_pb2.STATUS.PARTITION_START,
            services_pb2.STATUS.PARTITION_DONE,
            "Assigning Partition creation task to mappers...",
            "Partition Data creation succesfull by all mappers",
            math.lcm(self.mapper_num, self.reducer_num),
            PARTITION_DIR,
        )

        # # assigning partition -> shuffle and sorting (output)
        time.sleep(2)
        self.handle_communication(
            "Reducer",
            PARTITION_DIR,
            self.reducer_stubs,
            services_pb2.STATUS.SUFFLE_SORT_START,
            services_pb2.STATUS.SUFFLE_SORT_DONE,
            "Assigning shuffle & sort task to reducer...",
            "shuffle & sort succesfull by all reducer",
            self.reducer_num,
            self.output_dir,
            True,
        )

        print("\n\n------------------- Feature completed -------------------")
        print(f"\n\n------ See '{self.output_dir}' directory for Result -------")

    def handle_communication(
        self,
        worker_type,
        in_dir,
        stub_map,
        given_task,
        target_task_status,
        task_start_log,
        task_done_log,
        odfn,
        out_file=None,
        shuffle=False,
    ):
        print("\n", "-" * 50)
        print(f"[-] {task_start_log}\n")

        files = self.get_all_files_from_a_dir(in_dir)
        file_task_status = dict()
        for file in files:
            file_task_status[file] = FILE_NOT_ASSIGNED

        while True:
            # shuffle indexes in case of reducer to access files
            indexs = self.shuffle(shuffle, len(files))
            for i in indexs:
                file = files[i]
                if file_task_status[file] == FILE_TASK_DONE:
                    continue

                if file_task_status[file] == FILE_NOT_ASSIGNED:
                    free_worker = self.get_IDLE_worker(stub_map)
                    if free_worker == -1:
                        continue

                    out_f = f"{INTERIM_DIR}/interim_{i+1}.txt"
                    if shuffle:
                        out_f = f"{out_file}/output_{free_worker}.txt"
                    elif out_file is not None:
                        out_f = out_file

                    file_task_status[file] = free_worker
                    stub_map[free_worker].assign_worker_a_task(
                        services_pb2.ASSIGN_TASK(
                            feature=self.feature,
                            task=given_task,
                            input_file=f"{in_dir}/{file}",
                            output_file=out_f,
                            output_dir_files_num=odfn,
                        )
                    )
                else:
                    # the task is already assigned to a worker
                    try:
                        worker_assigned = file_task_status[file]
                        res = stub_map[worker_assigned].get_worker_status(
                            empty_pb2.Empty(), timeout=5
                        )

                        if res.status == target_task_status:
                            print(
                                f"- {worker_type} #{worker_assigned} assinged file '{file}'"
                            )
                            file_task_status[file] = FILE_TASK_DONE
                            stub_map[worker_assigned].set_worker_status(
                                services_pb2.WORKER_STATUS(
                                    status=services_pb2.STATUS.IDLE
                                )
                            )
                    except grpc.RpcError as e:
                        if e.code() == grpc.StatusCode.UNAVAILABLE:
                            file_task_status[file] = FILE_NOT_ASSIGNED

            file_done_num = sum(_ == FILE_TASK_DONE for _ in file_task_status.values())
            if file_done_num == len(files):
                break

        print(f"\n[+] {task_done_log}")

    ############################################################################

    def create_stubs(self, address, port):
        channel = grpc.insecure_channel(f"{address}:{port}")
        stub = services_pb2_grpc.MapperReducerStub(channel)
        return stub

    def get_all_files_from_a_dir(self, dir_path):
        all_files = list()
        for file in os.listdir(dir_path):
            all_files.append(file)
        return all_files

    # return worker_id of a worker if available, else -1
    def get_IDLE_worker(self, stubs: dict):
        for worker_id, stub in stubs.items():
            try:
                res = stub.get_worker_status(empty_pb2.Empty(), timeout=5)
                if res.status == services_pb2.STATUS.IDLE:
                    return worker_id
            except grpc.RpcError as e:
                pass
        return -1

    def shuffle(self, shuffle: bool, len: int):
        indexs = [_ for _ in range(0, len)]
        if shuffle:
            random.shuffle(indexs)
        return indexs
