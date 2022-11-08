from eme.pipe import Process
from metabolite_index.edb_formatting import preprocess, remap_keys, split_pubchem_ids, map_to_edb_format, MultiDict

from builder_pipe.dtypes.MetaboliteExternal import MetaboliteExternal
from builder_pipe.process.bulkparsers.utils import assert_edb_dict


class DebugProd(Process):
    consumes = str, "filename"
    produces = MetaboliteExternal, "edb_record"

    async def produce(self, data: MultiDict):
        ff = [
            {'edb_source': 'pubchem', 'pubchem_id': '52925597', 'chebi_id': None,
             'hmdb_id': None, 'kegg_id': None, 'lipidmaps_id': None, 'cas_id': None, 'chemspider_id': None,
             'metlin_id': None, 'swisslipids_id': None,
             'smiles': 'CCCCCCCCCC=CCCCCCCCC(=O)OCC(COP(=O)(O)OCC(C(=O)O)N)OC(=O)CCCCCCCC=CCCCCCC',
             'inchi': '1S/C41H76NO10P/c1-3-5-7-9-11-13-15-17-18-19-21-22-24-26-28-30-32-39(43)49-34-37(35-50-53(47,48)51-36-38(42)41(45)46)52-40(44)33-31-29-27-25-23-20-16-14-12-10-8-6-4-2/h14,16,18-19,37-38H,3-13,15,17,20-36,42H2,1-2H3,(H,45,46)(H,47,48)/b16-14-,19-18-/t37-,38+/m1/s1',
             'inchikey': 'JPKMENBYEXXYOZ-BILDVAMGSA-N', 'formula': 'C41H76NO10P', 'charge': '0', 'mass': '774.0',
             'mi_mass': '773.52068462',
             'names': [
                 "(2<I>S</I>)-2-amino-3-[[(2<I>R</I>)-2-[(<I>Z</I>)-hexadec-9-enoyl]oxy-3-[(<I>Z</I>)-nonadec-9-enoyl]oxypropoxy]-hydroxyphosphoryl]oxypropanoic acid",
                 "(2S)-2-amino-3-[hydroxy-[(2R)-2-[(Z)-1-oxohexadec-9-enoxy]-3-[(Z)-1-oxononadec-9-enoxy]propoxy]phosphoryl]oxypropanoic acid",
                 "(2S)-2-amino-3-[[(2R)-2-[(Z)-hexadec-9-enoyl]oxy-3-[(Z)-nonadec-9-enoyl]oxy-propoxy]-hydroxy-phosphoryl]oxy-propionic acid",
                 "(2S)-2-amino-3-[[(2R)-2-[(Z)-hexadec-9-enoyl]oxy-3-[(Z)-nonadec-9-enoyl]oxypropoxy]-hydroxyphosphoryl]oxypropanoic acid",
                 "(2S)-2-amino-3-[[(2R)-2-[(Z)-hexadec-9-enoyl]oxy-3-[(Z)-nonadec-9-enoyl]oxy-propoxy]-hydroxy-phosphoryl]oxy-propanoic acid",
                 "(2S)-2-azanyl-3-[[(2R)-2-[(Z)-hexadec-9-enoyl]oxy-3-[(Z)-nonadec-9-enoyl]oxy-propoxy]-oxidanyl-phosphoryl]oxy-propanoic acid"
             ],
             'description': None,
             'attr_mul': {"smiles": ["CCCCCCCCC/C=C\\CCCCCCCC(=O)OC[C@H](COP(=O)(O)OC[C@@H](C(=O)O)N)OC(=O)CCCCCCC/C=C\\CCCCCC"]},
             'attr_other': {}},
        ]

        for e in ff:
            yield MetaboliteExternal(**e)
