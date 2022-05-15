
class ReadDataUtil:


    def readCsv(self,spark,path,schema,inferschema=True,header=True,sep=","):
        '''
        returns new dataframe by reading provided csv file
        :param spark:spark session
        :param path:csv file path or directory path
        :param schema=provide schema, required when inferschema is false
        :param inferschema: if true :detect file schema  else false: ignore auto detect schema
        :param header: if true input csv file has header
        :param sep:"," specify seperator present in csv file
        :return:
        '''
        # if (inferschema is False) and (schema == None):
        #     raise Exception ("please provide inferschema as true else provide schema for given input file")
        readdf= spark.read.csv(schema,path=path,inferSchema=inferschema,header=header,sep=sep)
        return readdf