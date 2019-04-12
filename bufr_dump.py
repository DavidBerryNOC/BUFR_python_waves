from bufr_message import *
import bitarray
import sys

def main( argv ):
    bufr_tables = './BUFR_TABLES/'
    file_list = ['dws-drifter-first5_v2.bufr']

    file_count = 0
    for f in file_list:
        fh = open( f, 'rb')
        bits = bitarray.bitarray()
        bits.fromfile(fh)
        fh.close()

        # find start of BUFR message
        idx = 0
        nbits = len(bits)
        while idx < (nbits - 32):
            if ((bits[idx:(idx + 32)].tobytes() == b'BUFR')):
                break
            idx += 1

        msg = bufr_message(bufr_tables + '/BUFRCREX_31_0_0_TableB_en.txt',
                           bufr_tables + '/BUFR_31_0_0_TableD_en.txt')

        try:
            msg.read_message( bits[idx:nbits] )
        except:
            print( "Error reading : " + f)
            print(idx)
            print(nbits)
            continue
        s = msg.expand_sequence( msg.section3['unexpanded_descriptors'])

        pd.set_option('display.max_columns' , 5)
        pd.set_option('display.width' , 200)
        pd.set_option('display.max_rows' , 5000)



        fh2 = open('dump{}.txt'.format(file_count),'w')

        print( "============================= section 0 =============================", file = fh2)
        for key in msg.section0 :
            print( key , ':\t\t' , msg.section0[key], file = fh2)
        print( "============================= section 1 =============================", file = fh2)
        for key in msg.section1 :
            print( key , ':\t\t' , msg.section1[key], file = fh2)
        print( "============================= section 3 =============================", file = fh2)
        for key in msg.section3 :
            print( key , ':\t\t' , msg.section3[key], file = fh2)
        print( "============================= section 4 =============================", file = fh2)
        for key in msg.section4:
            if key != 'payload':
                print(key, ':\t\t', msg.section4[key], file = fh2)
        for subset in range(msg.section3['number_subsets']):
            ss = msg.read_expanded_sequence(s, msg.section4['payload'])
            ss.reset_index(inplace=True, drop=True)
            print("/////======================== subset: ",subset," =============================", file = fh2)
            print( ss , file = fh2)

            fh = open('t5.txt','w')
            count = 0
            for index, row in ss.iterrows():
                if (row['FXY'][0] == '0') & (row['FXY'][0:3] != '031'):
                    if row['Units'] == 'CCITT IA5':
                        fh.write('{}'.format( row['Value']))
                    else:
                        fh.write('{:14.8f}'.format(float(row['Value']) ) )
                    count += 1
                    if count == 8:
                        fh.write('\n')
                        count = 0
                    else:
                        fh.write(',')
            fh.close()
            file_count += 1
        print( "============================= section 5 =============================", file = fh2)
        for key in msg.section5 :
            print( key , ':\t\t' , msg.section5[key], file = fh2)
        print( "=============================    end    =============================", file = fh2)

        outfile = msg.section3['unexpanded_descriptors'][0] + "_" + \
                  str( len(s) ) + \
                  "_" +  str(file_count) + '.csv'


        fh2.close()



if __name__ == '__main__':
    main(sys.argv[1:])