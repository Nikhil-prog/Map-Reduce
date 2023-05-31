STORAGE_DIR = 'Storage'
INTERIM_DIR = f'{STORAGE_DIR}/Intermediate'
PARTITION_DIR =  f'{STORAGE_DIR}/Partition'

FILE_NOT_ASSIGNED = "NA"
FILE_TASK_DONE = "DONE"


 # stub = self.mapper_stubs[1]
        # while True:
        #     time.sleep(2)
        #     try:
        #         # response = stub.MyMethod(myservice_pb2.MyRequest(param="value"), timeout=5)
        #         res = stub.get_worker_status(empty_pb2.Empty(), timeout=5)
        #         print(res.status)
        #     except grpc.RpcError as e:
        #         if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        #             print("Timed out waiting for response")
        #         elif e.code() == grpc.StatusCode.UNAVAILABLE:
        #             print("Server is not available")
        #         else:
        #             print("An error occurred:", e.details())
        #     except Exception as e:
        #         print("An unexpected error occurred:", e)