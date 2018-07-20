
class TestConf(object):
    def TargetDrug(self):
        return [
            "AZACITIDINE", "PREDNISONE", "CLADRIBINE", "ATRA", "TRISENOX",
            "METHYLPREDNISOLONE", "CYCLOPHOSPHAMIDE",
            "CYTARABINE", "CYTOSAR_U", "CERUBIDINE", "DECITABINE",
            "DOXORUBICIN" ,"IDAMYCIN", "NOVANTRONE", "GEMTUZUMAB",
            "ONCOVIN","FLUDARABINE", "THIOGUANINE", "VINCASAR_PFS",
            "MIDOSTAURIN", "SORAFENIB", "ENASIDENIB", "DAUNORUBICIN",
            "VENETOCLAX", "CLOFARABINE",
            "VIDAZA", "LEUSTATIN", "MAVENCLAD", "LEUSTAT", "DACOGEN", "ADRIAMYCIN", "IDARUBICIN", "MITOXANTRONE",
            "VINCRISTINE", "VINCASAR PFS", "MYLOTARG", "FLUDARA", "FLUDARABINE PHOSPHATE", "RYDAPT", "TABLOID",
            "NEXAVAR", "SORAFENIB TOSYLATE", "IDHIFA", "AG-221", "VYXEOS", "NEOSAR", "MEDROL", "SOLU-MEDROL",
            "DEPO-MEDROL", "MEPREDNISONE", "NEUPOGEN", "REFISSA", "ATRALIN", "TRETIN-X", "TRETINOIN", "ARSENIC TRIOXIDE",
            "SGI-110", "VENCLEXTA", "ABT-199", "GDC-199", "CLOLAR", "EVOLTRA", "AG-120"
        ]

    def TargetProcedure(self):
        return ['AML_FISH_PANEL', 'AML_PROGNOSTIC_PANEL', 'STEM_CELL_TRANSPLANT', 'BONE_MARROW_TRANSPLANT']


# class AmlDrugs(smv.SmvCsvStringData):
#     def schemaStr(self):
#         return "drug:String;\
#                 brand:String;\
#                 generic:String;\
#                 mesh:String;\
#                 hcpcs:String;\
#                 type:String;\
#                 form:String;\
#                 filterB:Boolean;\
#                 filterD:Boolean;\
#                 filterPM:Boolean;\
#                 filterCT:Boolean;\
#                 showB:Boolean;\
#                 showD:Boolean;\
#                 showOP:Boolean"
#
#     def dataStr(self):
#         return """AZACITIDINE,"VIDAZA,AZACITIDINE",AZACITIDINE,D001374,J9025,,IV,False,False,True,True,True,True,True;
#                   CLADRIBINE,"LEUSTATIN,MAVENCLAD,LEUSTAT,CLADRIBINE",CLADRIBINE,D017338,J9065,,IV,False,False,True,True,True,True,True;
#                   CYTOSAR_U,CYTOSAR_U,CYTARABINE,D003561,J9100,,IV,False,False,True,True,True,True,True;
#                   CERUBIDINE,CERUBIDINE,DAUNORUBICIN,D003630,J9150,,IV,False,False,True,True,True,True,True;
#                   DECITABINE,DACOGEN,DECITABINE,C014347,J0894,Chemo,IV,False,False,True,True,True,True,True;
#                   DOXORUBICIN,ADRIAMYCIN,DOXORUBICIN,D004317,J9000,,IV,False,False,True,True,True,True,True;
#                   IDAMYCIN,IDAMYCIN,IDARUBICIN,D015255,J9211,,IV,False,False,True,True,True,True,True;
#                   NOVANTRONE,NOVANTRONE,MITOXANTRONE,D008942,J9293,,IV,False,False,True,True,True,True,True;
#                   ONCOVIN,ONCOVIN,VINCRISTINE,D014750,J9370,,IV,False,False,True,True,True,True,True;
#                   VINCASAR_PFS,VINCASAR PFS,VINCRISTINE,D014750,J9370,,IV,False,False,True,True,True,True,True;
#                   GEMTUZUMAB,MYLOTARG,GEMTUZUMAB,C406061,J9300,,IV,False,False,True,True,True,True,True;
#                   FLUDARABINE,FLUDARA,FLUDARABINE PHOSPHATE,C042382,"J9185,J8562",,Oral,False,False,True,True,True,True,True;
#                   MIDOSTAURIN,RYDAPT,MIDOSTAURIN,C059539,,,Oral,True,True,True,True,True,True,True;
#                   THIOGUANINE,TABLOID,THIOGUANINE,D013866,,,Oral,False,False,True,True,True,True,True;
#                   SORAFENIB,NEXAVAR,SORAFENIB TOSYLATE,C471405,,,Oral,False,False,True,True,True,True,True;
#                   ENASIDENIB,IDHIFA,"ENASIDENIB,AG-221",C000605269,,,Oral,True,True,True,True,True,True,True;
#                   DAUNORUBICIN,VYXEOS,DAUNORUBICIN,D003630,,,Oral,True,True,True,True,True,True,True;
#                   CYTARABINE,"VYXEOS,CYTARABINE",CYTARABINE,D003561,,,Oral,True,True,True,True,True,True,True;
#                   CYCLOPHOSPHAMIDE,NEOSAR,CYCLOPHOSPHAMIDE,D003520,,,IV/Oral,False,False,True,True,True,True,True;
#                   PREDNISONE,PREDNISONE,PREDNISONE,D011241,"J7512,J7506",Steroid,Oral,False,False,False,False,False,False,False;
#                   METHYLPREDNISOLONE,"MEDROL,SOLU-MEDROL,DEPO-MEDROL,METHYLPREDNISOLONE","METHYLPREDNISOLONE,MEPREDNISONE",D008775,"J1020,J030,J040",Steroid,Oral,False,False,False,False,False,False,False;
#                   FILGRASTIM,"NEUPOGEN,FILGRASTIM",FILGRASTIM,D000069585,J1442,G-CSF,IV,False,False,True,True,True,True,True;
#                   ATRA,"ATRA,REFISSA,ATRALIN,TRETIN-X,TRETINOIN","TRETINOIN,ATRA",D014212,S0117,VITAMIN A DERIVATIVE,Oral/Topical,False,False,False,False,False,False,False;
#                   TRISENOX,TRISENOX,"ARSENIC TRIOXIDE",C006632,J9017,CHEMICAL,IV,False,False,False,False,False,False,False;
#                   ETOPOSIDE,ETOPOSIDE,ETOPOSIDE,D005047,J9181,Chemo,IV,True,True,True,True,True,True,True;
#                   GUADECITABINE,"GUADECITABINE,SGI-110",GUADECITABINE,DB11918,,Chemo,IV,True,True,True,True,True,True,True;
#                   VENETOCLAX,"VENETOCLAX,VENCLEXTA","VENETOCLAX,ABT-199,GDC-199",C579720,00074056111,Chemo,IV,True,True,True,True,True,True,True;
#                   CLOFARABINE,"CLOLAR,EVOLTRA",CLOFARABINE,C068329,J9027,Chemo,IV,True,True,True,True,True,True,True;
#                   IVOSIDNIB,IVOSIDNIB,"IVOSIDNIB,AG-120",,,Chemo,IV,True,True,True,True,True,True,True"""
