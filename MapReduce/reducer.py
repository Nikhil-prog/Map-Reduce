import services_pb2 as services_pb2
import services_pb2_grpc as services_pb2_grpc
from google.protobuf import empty_pb2

import os


class Reducer(services_pb2_grpc.MapperReducerServicer):
    def __init__(self, address, port) -> None:
        print("------------------- Reducer Initialise -------------------\n")
        print(f"[+] Reducer #{port-50100} is Live at {address}:{port}\n")

        self.worker_status = services_pb2.STATUS.IDLE
        self.curr_working_file = None

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
        if task == services_pb2.STATUS.SUFFLE_SORT_START:
            self.generate_output(feature_fun[feature], in_file, out_file)
        return empty_pb2.Empty()

    ######################### General FUNCTIONS ###########################

    def word_count(self, key_value: dict, ith_line: int, line: str):
        key, value = line.strip()[1:-1].split(",")
        if key in key_value.keys():
            key_value[key] += int(value)
        else:
            key_value[key] = int(value)

    def inverted_index(self, key_value: dict, ith_line: int, line: str):
        key, value = line.strip()[1:-1].split(",")
        if key in key_value.keys():
            new_value = f"{key_value[key]}, {value}"
            key_value[key] = new_value
        else:
            key_value[key] = value

    def natural_join(self, key_value: dict, ith_line: int, line: str):
        self.print_function_idx = 1
        key, tem = line.strip()[1:-1].split(",")

        for x in tem.strip().split(";"):
            age_or_role, value = x.strip()[1:-1].split("@")
            if key in key_value.keys():
                if age_or_role in key_value[key].keys():
                    key_value[key][age_or_role].append(value)
                else:
                    key_value[key][age_or_role] = [value]
            else:
                key_value[key] = {age_or_role: [value]}

    def word_count_and_inverted_index_print(self, out_file: str, key_value: dict):
        f = open(out_file, "a")
        for key, value in key_value.items():
            f.write(f"{key} : {value}\n")
        f.close()

    def natural_join_print(self, out_file: str, key_value: dict):
        # if file is just created
        if not os.path.exists(out_file):
            f = open(out_file, 'a')
            f.write("Name,\t Age,\t Role\n")

        f = open(out_file, "a")
        for key in key_value.keys():
            subdict = key_value[key]

            # if a key does not have either age or role
            # skip it
            if ("Age" not in subdict.keys()) or ("Role" not in subdict.keys()):
                continue

            ages = subdict["Age"]
            roles = subdict["Role"]

            for _age in ages:
                for _role in roles:
                    f.write(f"{key},\t {_age},\t {_role}\n")

        f.close()

    def generate_output(self, feature_fun, in_file: str, out_file: str):
        print(f"[+] Performing shuffle & sort -> Output Task on file: {in_file}")
        self.worker_status = services_pb2.STATUS.SUFFLE_SORT_WORKING

        self.print_function_idx = 0
        key_value = dict()

        f1 = open(in_file, "r")
        for i, line in enumerate(f1.readlines()):
            if not len(line):
                continue

            # calling feature function
            feature_fun(key_value, i, line)

        f1.close()

        possible_print_fun = [
            self.word_count_and_inverted_index_print,
            self.natural_join_print,
        ]
        # printing the result to output file
        print_func = possible_print_fun[self.print_function_idx]
        print_func(out_file, key_value)

        print(f"[+] Done shuffle & sort -> Output Task on file: {in_file}")
        self.worker_status = services_pb2.STATUS.SUFFLE_SORT_DONE
