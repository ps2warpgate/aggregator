import multiprocessing


for process in ('rest_client', 'ess_client'):
    p = multiprocessing.Process(target=lambda: __import__(process))
    p.start()