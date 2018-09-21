import otf2
from otf2.events import *
import numpy as np
import pandas as pd

def get_metric_events(trace_name):
    with otf2.reader.open(trace_name) as trace:
        metric_events = []
        for metric_members in trace.definitions.metric_members:
            metric_events.append(metric_members.name)
    return metric_events

def open_trace(trace_name):
    with otf2.reader.open(trace_name) as trace:
        for location,event in trace.events:
            yield event

def get_count_phase_num(trace_name,phase_name):
    event = open_trace(trace_name)
    count = 0
    for i in event:
        if isinstance(i, Enter):
            if(i.region.name == phase_name):
                count +=1
        elif isinstance(i, Leave):
            if(i.region.name == phase_name):
                count += 1;
    return count/2

def get_papi_values(trace_name, papi_events, num_phase_iter, num_processes):
    event = open_trace(trace_name)
    values_list = [0]*len(papi_events)
    count = 0
    temp_count = 0
    # print(num_processes)
    # print(num_phase_iter)
    if(num_processes <= (num_phase_iter/num_processes)):
        print("First loop is being executed")
        for event_ in event:
            if isinstance(event_, Metric):
                if(len(event_.values) == len(papi_events)):
                    count +=1
                    if(count > num_phase_iter*2 - int(num_processes)):
                        temp_count += 1
                        # print(values_list)
                        for i in range(0, len(papi_events)):
                            values_list[i] += event_.values[i]
                        print(values_list)
    else:
        print("Second loop is being executed")
        for event_ in event:
            if isinstance(event_, Metric):
                if(len(event_.values) == len(papi_events)):
                    print(event_)
                    count +=1
                    if(count > num_processes):
                        temp_count +=1
                        for i in range(0, len(papi_events)):
                            values_list[i] += event_.values[i]
                        print(values_list)

    print(temp_count)
    values_list = [i/num_processes for i in values_list]
    temp_value = num_phase_iter/num_processes
    values_list = [i/temp_value for i in values_list]

    return values_list

def get_papi_values_w_time_stamps(time_stamps, papi_events, trace_name):
    event = open_trace(trace_name)
    values_list = [0]*len(papi_events)
    count = 0
    time_stamps.sort(key=int)
    # print(len(time_stamps))
    for event_ in event:
        if isinstance(event_, Metric):
            if(count <= len(time_stamps) - 1 and time_stamps[count] == event_.time):
                count += 1
                for i in range(0, len(papi_events)):
                    values_list[i] += event_.values[i]
                # print(values_list)

    values_list = [i/len(time_stamps) for i in values_list]
    return values_list


def get_time_stamps(trace_name, phase_region, num_processes, num_phase_iter):
    event = open_trace(trace_name)
    time_stamps =[]
    count = 0
    for event_ in event:
        if isinstance(event_, Enter) or isinstance(event_, Leave):
            if (event_.region.name == phase_region):
                count +=1
                # print(count)
                if(count > num_phase_iter*2 - int(num_processes)):
                    time_stamps.append(event_.time)

    # print(time_stamps)
    return time_stamps

def get_energy_values(trace_name, other_events):
    event = open_trace(trace_name)
    metric_values = np.zeros(len(other_events))
    metric_events_counts = np.zeros(len(other_events))
    time_list = []
    for event_ in event:
        if isinstance(event_, Metric):
            for i in range(0, len(other_events)):
                if(len(event_.values) == 1):
                    if(event_.metric.metric_class.members[0].name == other_events[i]):
                        metric_values[i] += event_.values[0]
                        metric_events_counts[i] += 1
                        time_list.append(event_.time)

    for i in range(0, len(other_events)):
        metric_values[i] /= metric_events_counts[i]
    return metric_values, time_list

def read_trace(trace_name, phase_region, name, num_processes):
    count = 0;
    time_list = []
    metric_events = get_metric_events(trace_name)
    num_phase_iter = get_count_phase_num(trace_name, phase_region)
    papi_events = [i for i in metric_events if "PAPI" in i]
    other_events = [i for i in metric_events if i not in papi_events]
    print(num_phase_iter/float(num_processes))
    values_list_papi = get_papi_values(trace_name, papi_events, num_phase_iter, float(num_processes))
    values_list_hdeem, time_list = get_energy_values(trace_name,other_events)
    with otf2.reader.open(trace_name) as trace:
        global_offset = trace.definitions.clock_properties.global_offset
        resolution = trace.timer_resolution

    time_list.sort(key=int)
    time_end = time_list[len(time_list) -1]
    time_start = time_list[0]
    time_end = (time_end - global_offset)/resolution
    time_start = (time_start - global_offset)/resolution
    time = time_end - time_start
    convert_2_csv(papi_events, other_events,values_list_papi,values_list_hdeem,name,time, num_phase_iter/float(num_processes))

def convert_2_csv(papi_events, other_events, papi_values, metric_values, name, time, num_phase_iter):
    data = list(papi_values) + list(metric_values)
    print(data)
    columns = []
    for i in range(0, len(papi_events)):
        columns.append(papi_events[i])
    for i in range(0, len(other_events)):
        columns.append((other_events[i]))
    data_dict = {columns[i]:data[i] for i in range(0, len(data))}
    data_dict.update({'time':time})
    data_dict.update({'Number of Phase Iterations':num_phase_iter})
    print(data_dict)

    df = pd.DataFrame(data = data_dict, index = [0])
    df.to_csv(name + ".csv", sep='\t', header=True)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description = 'This script parsers the metrics from an OTF2 trace file and converts it to a csv')
    parser.add_argument("-i","--input", help="Trace file with path", required=True)
    parser.add_argument("-p", "--phase_region", help="Name of phase region", required=True)
    parser.add_argument("-n", "--name", help="Name of the output csv file", required=True)
    parser.add_argument("-np","--num_processes", help="Number of MPI processes", required=True)
    args = parser.parse_args()
    read_trace(args.input, args.phase_region, args.name, args.num_processes)

