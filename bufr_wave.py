from expand_sequence import *
import json
import sys
from bitarray import bitarray
def main( argv ):
    pd.set_option('display.max_columns', 150)
    pd.set_option('display.max_rows', 200)
    pd.set_option('display.width', 300 )

    # Load message headers / sections
    msg_file = 'waverider_first_five.json'
    with open(msg_file) as bufrmsg:
        msg = json.load( bufrmsg )

    # ----------------------------------------------------------------------------------
    # need to do the next code block better, at the moment every subset has to have the
    # same set of delayed replicators. In future need to loop over different subsets
    # separately.
    # ----------------------------------------------------------------------------------

    # read in data file
    datain = pd.read_csv(msg['datafile'])
    replicators = [ datain.shape[0] ]
    nsubsets = 1
    # get descriptors to pack
    descriptors = expand_sequence( msg['section3']['descriptors']['value'], replicators )
    descriptors.reset_index( inplace=True )
    print(descriptors)
    # calculate message length
    data_length = descriptors.BUFR_DataWidth_Bits.sum() * nsubsets
    if data_length % 8 > 0:
        data_length = data_length + (8 - data_length % 8)
    print(data_length)
    data_length = int( data_length / 8)
    # ----------------------------------------------------------------------------------
    msg['section4']['length']['value'] = data_length + 4
    msg['section4']['data']['width'] = data_length
    msg_length = 0
    msg_length += 8
    msg_length += msg['section1']['length']['value']
    if msg['section1']['optional_section']['value'] == 1:
        msg_length += msg['section2']['length']['value']
    msg_length += msg['section3']['length']['value']
    msg_length += msg['section4']['length']['value']
    msg_length += 4


    # Set length in section 0
    msg['section0']['length']['value'] = msg_length

    # Pack data section

    # first we need to transpose and unstack our data frame to single series
    # get columns to write
    towrite = datain.loc[:,['freq','bandwidth','energy','a1','b1','a2','b2','check_factor']].copy()
    towrite = towrite.values.flatten()
    # add number of repeats at start
    replicators = [ datain.shape[0] ]
    towrite = pd.np.insert( towrite, 0, replicators[0])
    # now convert back to data frame
    towrite = pd.DataFrame({'data':towrite}).transpose()
    # ----------------------------------------------------------------------------------
    # move to pack_section function
    # ----------------------------------------------------------------------------------
    rsum = 0
    for i in range( towrite.shape[1] ):
        scale  = descriptors.loc[i,'BUFR_Scale']
        width  = descriptors.loc[i,'BUFR_DataWidth_Bits']
        rsum += width
        offset = descriptors.loc[i,'BUFR_ReferenceValue']
        units  = descriptors.loc[i,'BUFR_Unit']
        msng   = pow(2, width) - 1
        fld = towrite.columns[i]
        if units == 'CCITT IA5':
            towrite[fld] = towrite[fld].apply( lambda x: ''.join( format( ord(y), 'b').zfill(8) for y in x.rjust( int(width/8)) ))
            towrite[fld] = towrite[fld].apply( lambda x: x.zfill( width ) )
        else:
            towrite[fld] = towrite[fld].apply( lambda x: int(round(x * pow(10,scale) - offset)))
            towrite[fld] = towrite[fld].fillna( msng )
            towrite[fld] = towrite[fld].apply( lambda x: format( int(x), 'b').zfill( width ) )

    bitsOut = towrite.apply( lambda x: x.sum(), axis = 1).sum()
    nbytes = int( len(bitsOut) / 8 )
    pack = len(bitsOut) % 8
    if pack > 0:
        bitsOut = bitsOut + ''.zfill( 8 - pack )
    nbytes = int( len(bitsOut) / 8 )

    assert nbytes == data_length

    msg['section4']['data']['value'] = bitsOut

    # Write to file

    bitsOut = ''
    # pack sections
    bitsOut += pack_section( msg['section0'] )
    bitsOut += pack_section( msg['section1'] )
    if msg['section1']['optional_section']['value'] == 1:
        bitsOut += pack_section( msg['section2'] )
    bitsOut += pack_section( msg['section3'] )
    bitsOut += pack_section( msg['section4'] )
    bitsOut += pack_section( msg['section5'] )

    # now write to file
    #file_out = open('waves_first_five.bin', 'wb')
    file_out = open(msg['outputfile'], 'wb')
    bitarray( bitsOut ).tofile(file_out)
    file_out.close()

if __name__ == '__main__':
    main(sys.argv[1:])