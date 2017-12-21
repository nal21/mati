import queue
import json
import requests
import threading
import time
from labjack import ljm
from datetime import datetime
from uuid import getnode as getmac


DATA_BYTE_SIZE = 2
NUM_READS = 10
SERVER_BASE_URL = 'https://mati.engr.utk.edu/api'
CONFIG_PATH = '/config'
DATA_PATH = '/data'
DATA_HEADER = {'Content-Type': 'application/json'}


def get_device_mac():
    return str(hex(getmac())[2:])

def setup_devices():
    config = None
    ljm_devices = {}
    config_url = SERVER_BASE_URL + CONFIG_PATH + '?rpi_mac=' + get_device_mac()
    result = requests.get(config_url)
    if result.status_code == 200:
        config = result.json()[0]
        devices = config.get('devices', [])
        for device in devices:
            channels = [chnl for chnl in range(device.get('num_channels', 0))]
            channel_addrs = []

            for channel in channels:
                ain_start = device.get('starting_address', 0)
                channel_addrs.append(int(ain_start + DATA_BYTE_SIZE * channel))

            try:
                device_id = device.get('device_id', '')
                sample_rate = device.get('sample_rate', 0)
                read_rate = device.get('read_rate', 0)
                ljm_devices[device_id] = ljm.open(ljm.constants.dtANY, 
                                                  ljm.constants.ctANY,
                                                  device_id)

                ljm.eStreamStart(ljm_devices[device_id], read_rate, 
                                len(channel_addrs), channel_addrs, sample_rate)
            except:
                pass
    return (ljm_devices, config)

def stream_data(q, ljm_devices):
    try:
        # ljm.eWriteAddress(ljm_device, 1000, ljm.constants.FLOAT32, 2.5)
        remaining_data = {}
        #while True:
        for _ in range(NUM_READS):
            for device_id in ljm_devices:
                print("Reading from {0}...".format(device_id))
                results = ljm.eStreamRead(ljm_devices[device_id])
                read_time = str(datetime.now())
                reading = {"rpi_mac": get_device_mac(),
                           "device_id": device_id,
                           "read_time": read_time,
                           "data": results[0]}
                remaining_data[device_id] = results[1]
                q.put(reading)

        for device_id in ljm_devices:
            if (remaining_data.get(device_id, 0) > 0):
                print("Reading from {0}...".format(device_id))
                results = ljm.eStreamRead(ljm_devices[device_id])
                read_time = str(datetime.now())
                reading = {"rpi_mac": get_device_mac(),
                           "device_id": device_id,
                           "read_time": read_time,
                           "data": results[0]}
                q.put(reading)

            ljm.eStreamStop(ljm_devices[device_id])
    except ljm.LJMError as ljmerr:
        print("Error occurred while streaming. Quiting streaming program.")
        print(ljmerr)

    finally:
        for device_id in ljm_devices:
            ljm.close(ljm_devices[device_id])
        global is_reading
        is_reading = False


def write_data(q):
    global is_reading
    while not q.empty() or is_reading:
        if not q.empty():
            try:
                reading = q.get()
                data_url = SERVER_BASE_URL + DATA_PATH
                result = requests.post(data_url,
                                       headers=DATA_HEADER,
                                       json=reading)
                if (result.status_code != 201):
                    print(result.status_code)
                    break
                print("Writing data from {0} at {1}...".format(reading["device_id"], reading["read_time"]))
            except queue.Empty:
                continue
            except Exception as ex:
                print(ex)
                break
        else:
            time.sleep(1)

def main():
    num_channels = 1
    global is_reading
    is_reading = True
    q = queue.Queue()
    ljm_devices, config = setup_devices()

    read_thread = threading.Thread(target=stream_data, args=(q, ljm_devices))
    write_thread = threading.Thread(target=write_data, args=(q,))

    read_thread.start()
    write_thread.start()
    read_thread.join()
    write_thread.join()


if __name__ == '__main__':
    main()
