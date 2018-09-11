import os
def split_by_vin(in_file, out_dir):
    with open(in_file, 'r') as inf:
        line_num = 0
        for line in inf:
            line_num += 1
            records = line.split(',')
            # output file format is yyyymmdd/by_vin/vintype_vin.csv
            out_file = os.path.join(out_dir, '{}_{}.csv'.format(records[1], records[0]))
            if line_num % 10000 == 1:
                print('progress: line {}'.format(line_num))
            with open(out_file, 'a') as outf:
                outf.write(line)

files = [f for f in os.listdir('20180101') if 'part' in f]
for f in files:
    f = os.path.join('20180101', f)
    split_by_vin(f, '20180101/by_vin/')
    print('{} done'.format(f))
