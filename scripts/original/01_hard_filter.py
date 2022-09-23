import sys

def find_index(mylist,keyword):
    ind = -1
    for x in mylist:
        if keyword in x:
            ind = mylist.index(x)

    return ind

def main():
    if len(sys.argv) < 3:
        print("Usage:./hard_filter.py in.vcf out.vcf")
        exit(1)
    infile = sys.argv[1]
    outfile = sys.argv[2]
    lines = [l.strip() for l in open(infile)]

    of = open(outfile,"w")
    for l in lines:
        if l[0] == "#":
            of.write(l+'\n')
            continue
        x = l.split('\t')
        #print(x[0:8])
        ref = x[3]
        alt = x[4]
        filter = x[6]
        type = 0
        if len(ref) > 1 or len(alt) > 1:
            type = 1
        prop = x[7].split(';')
        fs_index = find_index(prop,"FS=")
        if fs_index != -1:
            fs = float(prop[fs_index].split("=")[1])
        else:
            fs = 201
        inbrd_index = find_index(prop,"InbreedingCoeff=")
        if inbrd_index !=-1:
            inbrd = float(prop[inbrd_index].split("=")[1])
        else:
            inbrd = -0.8
        mq_index = find_index(prop,"MQ=")
        if mq_index != -1:
            mq = float(prop[mq_index].split("=")[1])
        else:
            mq = 40.0
        mqrs_index = find_index(prop,"MQRankSum=")
        if mqrs_index != -1:
            mqrs = float(prop[mqrs_index].split("=")[1])
        else:
            mqrs = -12.5
        qd_index = find_index(prop,"QD=")
        if qd_index != -1:
            qd = float(prop[qd_index].split("=")[1])
        else:
            qd = 2.0
        rprs_index = find_index(prop,"ReadPosRankSum=")
        if rprs_index != -1:
            rprs = float(prop[rprs_index].split("=")[1])
        else:
            rprs = -0.8
        sor_index = find_index(prop,"SOR=")
        if sor_index != -1:
            sor = float(prop[find_index(prop,"SOR=")].split("=")[1])
        else:
            sor = 11
        #print(l)
        print(fs,inbrd,mq,mqrs,qd,rprs,sor)
        if type == 0:
            if fs < 60.0 and inbrd > -0.8 and mq > 40 and mqrs > -12.5 and qd > 2.0 and rprs > -8.0 and sor <= 3.0:
                of.write(l+'\n')
            else:
                print(type,fs < 60,inbrd > -0.8,mq > 40,mqrs > -12.5,qd > 2,rprs > -0.8,sor <=3.0)
            #print("exclude: "+l+'\n')
        else:
            if fs < 200 and qd > 2.0 and rprs > -20 and sor <=10:
                of.write(l+'\n')
            else:
                print(type,fs<200,qd>2,rprs>-20,sor<=10)
            #print("exclude: "+l+'\n')

    of.close()

if __name__ == "__main__":
    main()
