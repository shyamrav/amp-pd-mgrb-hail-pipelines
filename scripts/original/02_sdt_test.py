import sys
import os

def main():
    if len(sys.argv) < 3:
        exit(1)
    infile = sys.argv[1]
    famfile = sys.argv[2]
    fam = [l.strip() for l in open(famfile)]
    cases = []
    controls = []
    for l in fam:
        x = l.split()
        if x[5] == '2':
            cases.append(x[1])
        elif x[5] == '1':
            controls.append(x[1])


    scase = [0,0]
    scon = [0,0]
    dcase = [0,0]
    dcon = [0,0]
    dsh = [0,0]
    tcase = [0,0]
    tcon = [0,0]
    tsh = [0,0]

    lines = [l.strip() for l in open(infile)]
    for l in lines:
        x = l.split('\t')
        y = x[23].split(';')
        ncase = len([v for v in y if v in cases])
        ncontrol = len([v for v in y if v in controls])
        csq = x[6]
        ll = ncase + ncontrol
        if ll > 3:
            continue
        elif ll == 1:
            if csq == 'synonymous_variant':
                if ncase == 1:
                    scase[0] += 1
                elif ncontrol == 1:
                    scon[0] += 1
            elif csq == 'missense_variant':
                if ncase == 1:
                    scase[1] += 1
                elif ncontrol == 1:
                    scon[1] += 1
        elif ll == 2:
            if csq == 'synonymous_variant':
                if ncase == 2:
                    dcase[0] += 1
                elif ncontrol == 2:
                    dcon[0] += 1
                elif ncase == 1 and ncontrol == 1:
                    dsh[0] += 1
            elif csq == 'missense_variant':
                if ncase == 2:
                    dcase[1] += 1
                elif ncontrol == 2:
                    dcon[1] += 1
                elif ncase == 1 and ncontrol == 1:
                    dsh[1] += 1
        elif ll == 3:
            if csq == 'synonymous_variant':
                if ncase == 3:
                    tcase[0] += 1
                elif ncontrol == 3:
                    tcon[0] += 1
                elif (ncase == 2 and ncontrol == 1) or (ncase == 1 and ncontrol == 2):
                    tsh[0] += 1
            elif csq == 'missense_variant':
                if ncase == 3:
                    tcase[1] += 1
                elif ncontrol == 3:
                    tcon[1] += 1
                elif (ncase == 2 and ncontrol == 1) or (ncase == 1 and ncontrol == 2):
                    tsh[1] += 1

    print(scase, scon)
    print(dcase, dcon, dsh)
    print(tcase, tcon, tsh)

if __name__=="__main__":
    main()
