import re
import datetime

f = open('/home/polina/Documents/parsing_logs/sun7/oldievent.backup.log', 'r', encoding = 'cp1251')
file = f.readlines()

def filtr(file):
    for i in range(len(file)):
        line = file[i]
        pat = re.compile(r'^(2020/03/02).+')
        s = pat.search(line)
        if s:
            yield line

def com(gen, list):
    for x in gen:
        list.append(x)

filtred = []
gen0 = filtr(file)
com(gen0, filtred)

def oldi(file):
    for i in range(len(file)):
        line = file[i]
        pat1 = re.compile(r'(TYPE - ABI)')
        pat2 = re.compile(r'(TYPE - ACT)')
        s1 = pat1.search(line)
        s2 = pat2.search(line)
        if s1 or s2:
            yield line


all = []
gen1 = oldi(filtred)
com(gen1, all)

def abi(all):
    for i in range(len(all)):
        format1 = '%Y/%m/%d %H:%M:%S'
        line = all[i]
        d = line[0:19]
        date = datetime.datetime.strptime(d, format1)
        pat1 = re.compile(r'(FPL_INT=)\d+')
        pat2 = re.compile(r'(TYPE - ABI)')
        pat3 = re.compile(r'(KEY_DOC=)\S[^,]+')
        pat4 = re.compile(r'(TEXT=)\S[^,]+')
        s1 = pat1.search(line)
        s2 = pat2.search(line)
        s3 = pat3.search(line)
        s4 = pat4.search(line)
        if s2:
            yield date, s1.group(), s2.group(), s3.group(), s4.group()

abi_list = []
gen2 = abi(all)
com(gen2, abi_list)

def act(all):
    for i in range(len(all)):
        format1 = '%Y/%m/%d %H:%M:%S'
        line = all[i]
        d = line[0:19]
        date = datetime.datetime.strptime(d, format1)
        pat1 = re.compile(r'(FPL_INT=)\d+')
        pat2 = re.compile(r'(TYPE - ACT)')
        pat3 = re.compile(r'(KEY_DOC=)\S[^,]+')
        pat4 = re.compile(r'(TEXT=)\S[^,]+')
        s1 = pat1.search(line)
        s2 = pat2.search(line)
        s3 = pat3.search(line)
        s4 = pat4.search(line)
        if s2:
            yield date, s1.group(), s2.group(), s3.group(), s4.group()

act_list = []
gen3 = act(all)
com(gen3, act_list)

total = abi_list + act_list

def pairs(total):
    diff = datetime.timedelta(minutes = 30)
    for i in range(len(total)):
        line = total[i]
        date = line[0]
        id = line[1]
        type = line[2]
        key = line[3]
        text = line[4]
        for j in range(len(total)):
            line2 = total[j]
            date2 = line2[0]
            id2 = line2[1]
            type2 = line2[2]
            key2 = line2[3]
            text2 = line2[4]
            if (date2-date <= diff) and type == 'TYPE - ABI' and id == id2 and type2 == 'TYPE - ACT':
                yield line, line2

pairs_list = []
gen4 = pairs(total)
com(gen4, pairs_list)

def split_list(pairs_list):
    for i in range(len(pairs_list)):
        line = pairs_list[i][0]
        yield line


split_pairs =[]
gen5 = split_list(pairs_list)
com(gen5, split_pairs)

#Counting flights
def get_flights(abi_list):
    for i in range(len(abi_list)):
        line = abi_list[i]
        id = line[1]
        yield id

id_list = []
gen5 = get_flights(abi_list)
com(gen5, id_list)

def unique(list1):
    unigue_list = []
    for x in list1:
        if x not in unigue_list:
            unigue_list.append(x)
    for x in unigue_list:
        yield x

id = []
gen6 = unique(id_list)
com(gen6, id)

#Soting abi
def sorted_abi(id, abi_list):
    for i in range(len(id)):
        line = id[i]
        for q in range(len(abi_list)):
            line2 = abi_list[q]
            num = line2[1]
            if line == num:
                yield line2

new_abi = []
gen7 = sorted_abi(id, abi_list)
com(gen7, new_abi)

d = {elem[1]: elem for elem in new_abi}
result = list(d.values())

#Finding ABI without ACT
def compare(abi_list, split_pairs):
    for element in abi_list:
        if element not in split_pairs:
            yield element

no_act = []
gen8 = compare(result, split_pairs)
com(gen8, no_act)

def format(no_act):
    for i in range(len(no_act)):
        line = no_act[i]
        format1 = '%Y/%m/%d %H:%M:%S'
        d = line[0]
        date = datetime.datetime.strftime(d, format1)
        yield date, line[1], line[2], line[3], line[4]

no_pairs = []
gen9 = format(no_act)
com(gen9, no_pairs)

len_all = len(id)
len_act = len(act_list)
len_no_pairs = len(no_pairs)

def listTosring():
    for i in range(len(no_pairs)):
        line = no_pairs[i]
        str1 = " "
        yield str1.join(line)

output = []
gen10 = listTosring()
com(gen10, output)

string1 = 'За сутки обработано %s бортов' %len_all
string2 = 'Найдено %s сообщений ABI, по которым не ушли ACT или ушли с опозданием более 30 мин' %len_no_pairs
string3 = 'Ниже представлен список ABI сообщений, по которым не ушли ACT или ушли с опозданием более 30 мин'
string4 = ''

output.insert(0, string1)
output.insert(1, string2)
output.insert(2, string3)
output.insert(3, string4)

with open('/home/polina/Desktop/report2.txt', 'w') as file2:
    for item in output:
        file2.write("%s\n" % item)


