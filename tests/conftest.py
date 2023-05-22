import time
curtest_item = None
pause_list = []
def pytest_enter_pdb(config, pdb):
    global curtest_item
    global pause_list
    test_case = curtest_item._testcase

    if hasattr(test_case, "cluster"):
        cluster = test_case.cluster
        for server in cluster.servers:
            if server.running:
                if not server.paused:
                    print(f'pytest_enter_pdb pausing server {server.name}')
                    server.direct_pause()
                    pause_list.append(server)
        
    
def pytest_leave_pdb(config, pdb):
    global pause_list
    for server in pause_list:
        server.resume()
        print(f"pytest_leave_pdb resumed server {server.name}")
    time.sleep(0.001)
    pause_list = []
    
def pytest_runtest_setup(item):
    #print(f"\n\nHOOK, setting up {item}")
    global curtest_item
    curtest_item = item

