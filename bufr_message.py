import pandas as pd

class bufr_message:
    idx = 0
    current_subset = pd.DataFrame( {'ElementName':[],'FXY':[],'Value':[],'Units':[]} )
    def __init__(self, table_B, table_D):
        self.table_B = pd.read_csv(table_B,sep=',', dtype='object')
        self.table_B['BUFR_DataWidth_Bits'] = self.table_B['BUFR_DataWidth_Bits'].map(int)
        self.table_B['BUFR_Scale'] = self.table_B['BUFR_Scale'].map(int)
        self.table_B['BUFR_ReferenceValue'] = self.table_B['BUFR_ReferenceValue'].map(float)
        self.table_D = pd.read_csv(table_D, sep=',', dtype='object')

        self.table_B_tmp = self.table_B.copy()

    def expand_sequence(self, sequence):
        content = list()
        for d in sequence:
            if d[0] == '3':
                expanded_sequence = (list(self.table_D.loc[self.table_D['FXY1'] == d, 'FXY2'].copy()))
                for e in expanded_sequence:
                    content.append( self.expand_sequence( [e] ) )
            else:
                content.append(d)
        return (content)

    def read_expanded_sequence(self, sequence, bits, reset = True):
        assert isinstance(sequence, list)
        subset = pd.DataFrame( {'ElementName':[],'FXY':[],'Value':[],'Units':[]} )
        if reset:
            self.table_B_tmp = self.table_B.copy()
        sequence_length = len( sequence )
        sidx = 0
        while sidx < sequence_length:
            s = sequence[ sidx ]
            if isinstance(s, str ):
                s = [s]
            if len(s) == 1:
                F   = s[0][0]
                XX  = s[0][1:3]
                YYY = int(s[0][3:6])
                if F == '2': # operator
                    if   XX == '01': # add YYY - 128 bits to width other than CCITT IA5, code and flag tables
                        exclude = ['CCITT IA5', 'Code table', 'Flag table']
                        mask = ~ self.table_B_tmp['BUFR_Unit'].isin(exclude)
                        if YYY > 0:
                            self.table_B_tmp.loc[ mask , 'BUFR_DataWidth_Bits'] =  \
                                self.table_B_tmp.loc[ mask , 'BUFR_DataWidth_Bits'] + int(YYY) - 128
                        else:
                            self.table_B_tmp.loc[ mask , 'BUFR_DataWidth_Bits'] = self.table_B.loc[ mask , 'BUFR_DataWidth_Bits'].copy()
                    elif XX == '02': # add YYY - 128 bits to scale other than CCITT IA5, code and flag tables
                        exclude = ['CCITT IA5', 'Code table', 'Flag table']
                        mask = ~ self.table_B_tmp['BUFR_Unit'].isin(exclude)
                        if YYY > 0:
                            self.table_B_tmp.loc[ mask , 'BUFR_Scale'] =  \
                                self.table_B_tmp.loc[ mask , 'BUFR_Scale'] + int(YYY) - 128
                        else:
                            self.table_B_tmp.loc[ mask , 'BUFR_Scale'] = self.table_B.loc[ mask , 'BUFR_Scale'].copy()
                    else:
                        stop()
                    tmp_df = pd.DataFrame({
                        'ElementName': ['Operator'],
                        'FXY': [s[0]],
                        'Value': [None],
                        'Units': [None]
                    })
                    subset = pd.concat([subset, tmp_df])
                    sidx += 1
                elif s[0][0] == '1': # replication
                    # get number of descriptors to repeat
                    nelements = int(XX)
                    # get number of replications
                    replications = YYY
                    sidx += 1
                    if YYY == 0:
                        s = sequence[sidx]
                        assert len(s) == 1
                        assert s[0][0:3] == '031'
                        tmp_df = self.read_expanded_sequence( [s] , bits, reset = False )
                        sidx += 1
                        subset = pd.concat([subset, tmp_df])
                        replications = int( round(tmp_df['Value'][0] ) )

                    # copy elements to repeat
                    elements_to_repeat = sequence[ sidx : (sidx + nelements ) ]
                    # now read the data
                    for repeatition in range( replications ) :
                        tmp_df = self.read_expanded_sequence( elements_to_repeat , bits, reset = False )
                        subset = pd.concat([subset, tmp_df])
                    # update position index
                    sidx += nelements
                else:
                    # read value
                    element = self.table_B_tmp[ self.table_B_tmp['FXY'] == s[0] ]
                    element.reset_index(inplace=True)
                    assert element.shape[0] == 1
                    scale     = float(element.loc[0,'BUFR_Scale'])
                    width     = element.loc[0,'BUFR_DataWidth_Bits']
                    reference = float(element.loc[0,'BUFR_ReferenceValue'])
                    unit      = element.loc[0,'BUFR_Unit']
                    name      = element.loc[0,'ElementName_en']
                    if (bits[self.idx:(self.idx + width)]).all() :
                        val = None
                    else:
                        if unit == 'CCITT IA5':
                            val = bits[self.idx:(self.idx + width)].tobytes().decode('ascii')
                            val = val.strip()
                        else:
                            val = int(bits[self.idx:(self.idx + width)].to01(),2)
                            if unit not in ['CCITT IA5', 'Code table', 'Flag table']:
                                val = (val + reference)*pow(10,-scale)
                    self.idx += width
                    tmp_df = pd.DataFrame({'ElementName':[name],'FXY':[s[0]],'Value':[val], 'Units':[unit]})
                    subset = pd.concat(  [subset, tmp_df] )
                    sidx += 1
            else:
                tmp_df = self.read_expanded_sequence( s, bits, reset = False )
                subset = pd.concat([subset, tmp_df])
                sidx += 1
        return( subset )

    def read_section0(self, bits):
        assert len(bits) == 64
        bufr = bits[0:32].tobytes().decode('ascii')
        assert bufr == 'BUFR'
        length = int(bits[33:56].to01(), 2)
        edition = int(bits[56:64].to01(), 2)
        assert edition == 4
        section0 = {'bufr': bufr, 'length': length, 'version': edition}
        self.section0 = section0

    def read_section1(self, bits):
        length = int(bits[0:24].to01(), 2)
        assert len(bits) == length * 8
        master_table = int(bits[24:32].to01(), 2)
        originating_centre = int(bits[32:48].to01(), 2)
        sub_centre = int(bits[48:64].to01(), 2)
        update_sequence = int(bits[64:72].to01(), 2)
        optional_section = bits[72:80]
        data_category = int(bits[80:88].to01(), 2)
        international_sub_category = int(bits[88:96].to01(), 2)
        local_sub_category = int(bits[96:104].to01(), 2)
        master_table_version = int(bits[104:112].to01(), 2)
        local_table_version = int(bits[112:120].to01(), 2)
        year = int(bits[120:136].to01(), 2)  # 2
        month = int(bits[136:144].to01(), 2)
        day = int(bits[144:152].to01(), 2)
        hour = int(bits[152:160].to01(), 2)
        minute = int(bits[160:168].to01(), 2)
        second = int(bits[168:176].to01(), 2)

        optional_length = (length - 22) * 8
        if optional_length > 0:
            optional = int(bits[176:(176 + optional_length)].to01(), 2)
        else:
            optional = None

        section1 = {'master_table': master_table,
                    'originating_centre': originating_centre,
                    'sub_centre': sub_centre,
                    'update_sequence': update_sequence,
                    'optional_section': optional_section,
                    'data_category': data_category,
                    'international_sub_category': international_sub_category,
                    'local_sub_category': local_sub_category,
                    'master_table_version': master_table_version,
                    'local_table_version': local_table_version,
                    'year': year,
                    'month': month,
                    'day': day,
                    'hour': hour,
                    'minute': minute,
                    'second': second,
                    'optional': optional}
        self.section1 = section1

    def read_section2(self, bits):
        length = int(bits[0:24].to01(), 2)
        assert len(bits) == length * 8
        zero = int(bits[24:32].to01(), 2)
        assert zero == 0
        length_local = (length - 4) * 8
        if length_local > 0:
            local_use = bits[32:(32 + length_local)]
        else:
            local_use = None
        section2 = {'length': length, 'zero': zero, 'local_use': local_use}
        self.section2 = section2

    def read_section3(self, bits):
        length = int(bits[0:24].to01(), 2)
        assert len(bits) == length * 8
        zero = int(bits[24:32].to01(), 2)
        assert zero == 0
        number_subsets = int(bits[32:48].to01(), 2)
        flags = int(bits[48:56].to01(), 2)
        ndescriptors = int(round((length - 7) / 2))
        idx = 56
        unexpanded_descriptors = []
        for i in range(ndescriptors):
            F = int(bits[idx:(idx + 2)].to01(), 2)
            XX = int(bits[idx + 2:(idx + 8)].to01(), 2)
            YYY = int(bits[idx + 8:(idx + 16)].to01(), 2)
            descriptor = '{0:01d}{1:02d}{2:03d}'.format(F, XX, YYY)
            unexpanded_descriptors.append(descriptor)
            idx += 16
        section3 = {
            'length': length,
            'zero': zero,
            'number_subsets': number_subsets,
            'flags': flags,
            'ndescriptors': ndescriptors,
            'unexpanded_descriptors': unexpanded_descriptors
        }
        self.section3 = section3

    def read_section4(self, bits):
        length = int(bits[0:24].to01(), 2)
        assert len(bits) == length * 8
        zero = int(bits[24:32].to01(), 2)
        assert zero == 0
        length_payload = length * 8 - 32
        payload = bits[32:(32 + length_payload)]
        section4 = {
            'length': length,
            'zero': zero,
            'payload': payload
        }
        self.section4 = section4

    def read_section5(self, bits):
        assert len(bits) == 4 * 8
        sevens = bits[0:32].tobytes().decode('ascii')
        #assert sevens == '7777'
        section5 = {'sevens': sevens}
        self.section5 = section5

    def read_header(self, bits):
        idx = 0
        section_length = 64
        self.read_section0(bits[idx:idx + section_length])
        idx = idx + section_length
        section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
        self.read_section1(bits[idx:(idx + section_length)])
        idx = idx + section_length
        if self.section1['optional_section'][0]:
            section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
            self.read_section2(bits[idx:(idx + section_length)])
            idx = idx + section_length
        else:
            section2 = None
        section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
        self.read_section3(bits[idx:(idx + section_length)])

    def read_message(self, bits):
        idx = 0
        section_length = 64
        self.read_section0(bits[idx:idx + section_length])
        idx += section_length
        section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
        self.read_section1(bits[idx:(idx + section_length)])
        idx += section_length
        if self.section1['optional_section'][0]:
            section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
            self.read_section2(bits[idx:(idx + section_length)])
            idx = idx + section_length
        else:
            section2 = None
        section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
        self.read_section3(bits[idx:(idx + section_length)])
        idx += section_length
        section_length = int(bits[idx:(idx + 24)].to01(), 2) * 8
        self.read_section4(bits[idx:(idx + section_length)])
        idx += section_length
        section_length = 4 * 8
        self.read_section5(bits[idx:(idx + section_length)])


