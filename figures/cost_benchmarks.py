import matplotlib.pyplot as plt

# correspond to sandboxes (https://cs.brown.edu/courses/csci1380/sandbox) 1, 2, and 3
m0_engine_data_in_kb = [20, 2_000, 23_900]#20kb*num_pages] # this is how much data needed to be indexed across all of the pages crawled
m0_engine_time_in_s = [4.64, 169.741, 3_583.993]

# for all of Project Gutenberg w/ n=100
m6_engine_data_in_kb_100 = [0, 50_000_000] # estimated
m6_engine_time_in_s_100 = [0, 28_800] # estimated

plt.figure()
plt.title('Scaling of Engine Runtime by Number of Nodes (n)')
plt.plot(m0_engine_data_in_kb, m0_engine_time_in_s, label='n=1 (centralized)')
plt.plot(m6_engine_data_in_kb_100, m6_engine_time_in_s_100, label='n=100 (GAS)')
plt.xscale('log')
plt.yscale('log')
plt.xlabel('cumulative page data computed (kb)')
plt.ylabel('time to crawl and index (s)')
plt.grid(zorder=1)
plt.legend()
plt.show()
