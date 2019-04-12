import pandas as pd
bufr_tables = "./BUFR_TABLES/"
table_A = pd.read_csv(bufr_tables+'/BUFR_31_0_0_TableA_en.txt',sep=',', dtype='object')
table_B = pd.read_csv(bufr_tables+'/BUFRCREX_31_0_0_TableB_en.txt',sep=',', dtype='object')
table_B['BUFR_DataWidth_Bits'] = table_B['BUFR_DataWidth_Bits'].map(int)
table_B['BUFR_Scale'] = table_B['BUFR_Scale'].map(int)
table_B['BUFR_ReferenceValue'] = table_B['BUFR_ReferenceValue'].map(float)
table_C = pd.read_csv(bufr_tables+'/BUFR_31_0_0_TableC_en.txt',sep=',', dtype='object')
table_D = pd.read_csv(bufr_tables+'/BUFR_31_0_0_TableD_en.txt',sep=',', dtype='object')

operators = {
    '01':{'name':'change_data_width',         'field':'BUFR_DataWidth_Bits', 'value':0},
    '02':{'name':'change_scale',              'field':'BUFR_Scale',          'value':0},
    '03':{'name':'change_reference_value',    'field':'BUFR_ReferenceValue', 'value':0},
    '04':{'name':'add_associated_value',      'field':None,                  'value':0},
    '05':{'name':'signify_character',         'field':None,                  'value':0},
    '06':{'name':'signify_width',             'field':None,                  'value':0},
    '07':{'name':'increase_scale_ref_width',  'field':None,                  'value':0},
    '08':{'name':'change_width_of_ccitt',     'field':'BUFR_DataWidth_Bits', 'value':0},
}

def expand_sequence( descriptors, replicators ):
    # change return value to pandas df
    # FXY, kind, units, width, scale, offset
    return_value = pd.DataFrame( columns = table_B.columns )
    ix = 0
    while ix < len( descriptors ):
        if descriptors[ix][0] == '0':
            # get values from table B and apply operators
            tmpRow = table_B[ table_B['FXY'] == descriptors[ix] ].copy()
            for op in operators:
                if operators[ op ]['value'] > 0:
                    if op in ['01','02']: # add yyy - 128 bits / scale to field excluding code / flag tables and CCITT data
                        exclude = ['CCITT IA5','Code table', 'Flag table']
                        tmpRow.loc[  (~ tmpRow['BUFR_Unit'].isin(exclude)), operators[ op ]['field'] ] = \
                            tmpRow.loc[(~ tmpRow['BUFR_Unit'].isin(exclude)), operators[op]['field']] + operators[op]['value'] - 128
                    if op == '08' :
                        tmpRow.loc[ tmpRow['BUFR_Unit'] == 'CCITT IA5' , operators[ op ]['field'] ] = \
                            operators[op]['value'] * 8

            return_value = pd.concat( [return_value, tmpRow] )
            ix += 1
        elif descriptors[ix][0] == '1':
            nelements = int(descriptors[ix][1:3])
            nreplications = int( descriptors[ix][3:6])
            if nreplications == 0:
                #next item is delayed descriptor, add to return value
                return_value = pd.concat( [return_value, expand_sequence( [descriptors[ix+1]], replicators ) ] )
                nreplications = replicators.pop(0)
                items = descriptors[(ix+2):(ix+nelements+2)]
                for iy in range(nreplications):
                    return_value = pd.concat([return_value, expand_sequence( items , replicators )] )
                ix += nelements + 2
            else:
                items = descriptors[(ix+1):(ix+nelements+1)]
                for iy in range(nreplications):
                    return_value = pd.concat( [return_value, expand_sequence( items , replicators )])
                ix += nelements + 1
        elif descriptors[ix][0] == '2':
            xx  = descriptors[ix][1:3]
            yyy = descriptors[ix][3:6]
            operators[ xx ]['value'] = int( yyy )
            ix += 1
        else : #descriptors[ix][0] == '3' :
            items = list( table_D[ table_D['FXY1'] == descriptors[ix]].FXY2 )
            return_value = pd.concat( [return_value, expand_sequence( items , replicators )])
            ix += 1
    return( return_value )

def pack_section( section ):
    bits = ''
    # encode section 0 and add to bits
    for key in section:
        print( section[key] )
        if section[key]['width'] > 0:
            if section[key]['kind'] == 'CCITT IA5' :
                word = ''.join( format( ord(x), 'b').zfill(8) for x in section[key]['value'])
                word = word.rjust( section[key]['width'] * 8)
                bits += word
            elif section[key]['kind'] == 'bin':
                # data already binary
                bits += section[key]['value']
            else:
                # should be int or list
                if isinstance( section[key]['value'], int ):
                    if pd.np.isnan(section[key]['value']):
                        word = pow(2, section[key]['width'] * 8) - 1
                        word = format(word, 'b').zfill(section[key]['width'] * 8)
                    else:
                        word = format(int(section[key]['value']), 'b').zfill(section[key]['width'] * 8)
                    bits += word
                elif isinstance( section[key]['value'], list ):
                    assert key == 'descriptors'
                    for item in section[key]['value']:
                        F   = int(item[0])
                        XX  = int(item[1:3])
                        YYY = int(item[3:6])
                        word = ''
                        word += format( F  , 'b').zfill(2)
                        word += format( XX , 'b').zfill(6)
                        word += format( YYY, 'b').zfill(8)
                        bits += word
    return( bits )

