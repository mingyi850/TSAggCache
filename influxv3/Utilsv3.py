import time

def withTrace(doTrace, traceDict, key, operation):
        if not doTrace:
            return operation()
        start = time.time()
        result = operation()
        end = time.time()
        traceDict[key] = end - start
        return result