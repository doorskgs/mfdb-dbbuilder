import unittest
from unittest import mock

from mfdb_parsinglib.apihandlers.ChebiClient import ChebiClient
from mfdb_parsinglib.apihandlers.HMDBClient import HMDBClient
from mfdb_parsinglib.apihandlers.KeggClient import KeggClient
from mfdb_parsinglib.edb_formatting import pad_id, depad_id
from tests.unit.mocking.apis import mocked_requests_get


class ApiTests(unittest.TestCase):

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_hmdb(self, mock_get):
        client = HMDBClient()
        resp = client.fetch_api('0000027')

        exp_names = {
            'Tetrahydrobiopterin',
            "(-)-(6R)-2-Amino-6-((1R,2S)-1,2-dihydroxypropyl)-5,6,7,8-tetrahydro-4(3H)-pteridinone",
            "(6R)-L-Erythro-5,6,7,8-tetrahydrobiopterin", "(6R)-L-Erythro-tetrahydrobiopterin",
            "2-Amino-6-(1,2-dihydroxypropyl)-5,6,7,8-tetrahydoro-4(1H)-pteridinone", "5,6,7,8-Tetrahydrobiopterin",
            "6R-5,6,7,8-Tetrahydrobiopterin", "6R-BH4", "6R-L-5,6,7,8-Tetrahydrobiopterin", "R-THBP", "Sapropterina",
            "Sapropterinum", "Tetrahydrobiopterin", "5,6,7,8-erythro-Tetrahydrobiopterin",
            "5,6,7,8-tetrahydro-L-Erythrobiopterin", "5,6,7,8-Tetrahydrobiopterin, (S-(r*,s*))-isomer",
            "5,6,7,8-Tetrahydrodictyopterin", "6R-L-erythro-5,6,7,8-Tetrahydrobiopterin", "BPH4",
            "D-threo-Tetrahydrobiopterin", "THBP", "Kuvan", "Phenylalanine hydroxylase cofactor",
            "Sapropterin dihydrochloride", "tetrahydro-6-Biopterin", "2',4',5'-Trihydroxybutyrophenone", "Sapropterin",
            "Trihydroxybutyrophenone", "1-Butanone, 1-(2,4,5-trihydroxyphenyl)", "2,4,5-Trihydroxybutyrophenone",
            "(6R)-5,6,7,8-Tetrahydro-L-biopterin", "(6R)-5,6,7,8-Tetrahydrobiopterin", "(6R)-Tetrahydrobiopterin",
            "2-Amino-6-(1,2-dihydroxypropyl)-5,6,7,8-tetrahydro-4(3H)-pteridinone", "6R-Tetrahydro-L-biopterin",
            "6beta-5,6,7,8-Tetrahydro-L-biopterin",
            "L-erythro-Tetrahydrobiopterin",
            "(6R)-2-Amino-6-[(1R,2S)-1,2-dihydroxypropyl]-5,6,7,8-tetrahydro-4(1H)-pteridinone",
            '(6R)-2-amino-6-[(1R,2S)-1,2-dihydroxypropyl]-1,4,5,6,7,8-hexahydropteridin-4-one',
            'tetrahydrobiopterin'
        }

        self.assertIsNotNone(resp)

        self.assertEqual('59560', resp.chebi_id)
        self.assertEqual('C00272', pad_id(resp.kegg_id, 'kegg_id'))
        self.assertIsNone(resp.lipmaps_id)
        self.assertEqual('44257', resp.pubchem_id)
        self.assertEqual('HMDB0000027', pad_id(resp.hmdb_id, 'hmdb_id'))
        self.assertEqual('62989-33-7', resp.cas_id)
        self.assertEqual('40270', resp.chemspider_id)
        self.assertIsNone(resp.metlin_id)
        self.assertEqual('[H][C@@]1(CNC2=C(N1)C(=O)N=C(N)N2)[C@@H](O)[C@H](C)O', resp.smiles)
        self.assertEqual('1S/C9H15N5O3/c1-3(15)6(16)4-2-11-7-5(12-4)8(17)14-9(10)13-7/h3-4,6,12,15-16H,2H2,1H3,(H4,10,11,13,14,17)/t3-,4+,6-/m0/s1', depad_id(resp.inchi, 'inchi'))
        self.assertEqual('FNKQXYHWGSIFBK-RPDRRWSUSA-N', resp.inchikey)
        self.assertEqual('C9H15N5O3', resp.formula)
        self.assertEqual(241.2471, float(resp.mass))
        self.assertEqual(241.117489371, float(resp.mi_mass))
        self.assertEqual(exp_names, set(resp.names))
        self.assertEqual({}, resp.attr_mul)
        self.assertEqual({'state': "Solid"}, resp.attr_other)


    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_chebi(self, mock_get):
        client = ChebiClient()
        resp = client.fetch_api('15422')

        exp_names = {
            'Atriphos', 'H4atp', 'Adephos', "adenosine 5'-(tetrahydrogen triphosphate)", 'Adenosine triphosphate',
             'Adynol', 'ATP', 'Adetol', 'Fosfobion', "ADENOSINE-5'-TRIPHOSPHATE", 'Glucobasin', 'Myotriphos',
             'Triadenyl', 'Triphosphaden', 'Cardenosine', 'Atipi', "Adenosine 5'-triphosphate"
        }

        self.assertIsNotNone(resp)

        self.assertEqual('15422', resp.chebi_id)
        self.assertEqual('C00002', pad_id(resp.kegg_id, 'kegg_id'))
        self.assertIsNone(resp.lipmaps_id)
        self.assertIsNone(resp.pubchem_id)
        self.assertEqual('HMDB0000538', pad_id(resp.hmdb_id, 'hmdb_id'))
        self.assertEqual('56-65-5', resp.cas_id)
        self.assertIsNone(resp.chemspider_id)
        self.assertIsNone(resp.metlin_id)
        self.assertEqual('Nc1ncnc2n(cnc12)[C@@H]1O[C@H](COP(O)(=O)OP(O)(=O)OP(O)(O)=O)[C@@H](O)[C@H]1O', resp.smiles)
        self.assertEqual('1S/C10H16N5O13P3/c11-8-5-9(13-2-12-8)15(3-14-5)10-7(17)6(16)4(26-10)1-25-30(21,22)28-31(23,24)27-29(18,19)20/h2-4,6-7,10,16-17H,1H2,(H,21,22)(H,23,24)(H2,11,12,13)(H2,18,19,20)/t4-,6-,7-,10-/m1/s1', resp.inchi)
        self.assertEqual('ZKHQWZAMYRWXGA-KQYNXXCUSA-N', resp.inchikey)
        self.assertEqual('C10H16N5O13P3', resp.formula)
        #self.assertEqual(0, float(resp.charge))
        self.assertEqual(507.18100, float(resp.mass))
        self.assertEqual(506.99575, float(resp.mi_mass))
        self.assertEqual(exp_names, set(resp.names))
        self.assertEqual({'cas_id': ['56-65-5']}, resp.attr_mul) #todo: FIX REDUNDANT VALUES IN MultiDict
        self.assertEqual({'stars': "3"}, resp.attr_other)


    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_kegg(self, mock_get):
        client = KeggClient()
        resps = list(client.fetch_api_bulk(['C01390','C01197']))

        self.assertIsNotNone(resps)
        self.assertEqual(2, len(resps))

        resp = resps[0]
        self.assertIsNotNone(resp)

        self.assertEqual('16019', resp.chebi_id)
        self.assertEqual('C01390', pad_id(resp.kegg_id, 'kegg_id'))
        self.assertEqual('LMFA05000106', pad_id(resp.lipmaps_id, 'lipmaps_id'))
        self.assertIsNone(resp.pubchem_id)
        self.assertIsNone(resp.hmdb_id)
        self.assertEqual('556-82-1', resp.cas_id)
        self.assertIsNone(resp.chemspider_id)
        self.assertIsNone(resp.metlin_id)
        self.assertEqual('C5H10O', resp.formula)
        self.assertEqual(86.1323, float(resp.mass))
        self.assertEqual(86.0732, float(resp.mi_mass))
        self.assertEqual({
            "Prenol",
            "Prenyl alcohol",
            "3-Methyl-2-buten-1-ol"
        }, set(resp.names))
        self.assertEqual({}, resp.attr_mul)
        self.assertEqual({}, resp.attr_other)
