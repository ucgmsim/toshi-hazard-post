gt_description = "Source Logic Tree v8"

src_correlations = {
    "dropped_group": "PUY",
    "correlations": [
        [
            {"group": "Hik", "tag": "Hik TL, N16.5, b0.95, C4, s0.42"},
            {"group": "PUY", "tag": "Puy 0.7, N4.6, b0.902, C4, s0.28"},
        ],
        [
            {"group": "Hik", "tag": "Hik TL, N16.5, b0.95, C4, s1"},
            {"group": "PUY", "tag": "Puy 0.7, N4.6, b0.902, C4, s1"},
        ],
        [
            {"group": "Hik", "tag": "Hik TL, N16.5, b0.95, C4, s1.58"},
            {"group": "PUY", "tag": "Puy 0.7, N4.6, b0.902, C4, s1.72"},
        ],
        [
            {"group": "Hik", "tag": "Hik TL, N21.5, b1.097, C4, s0.42"},
            {"group": "PUY", "tag": "Puy 0.7, N4.6, b0.902, C4, s0.28"},
        ],
        [
            {"group": "Hik", "tag": "Hik TL, N21.5, b1.097, C4, s1"},
            {"group": "PUY", "tag": "Puy 0.7, N4.6, b0.902, C4, s1"},
        ],
        [
            {"group": "Hik", "tag": "Hik TL, N21.5, b1.097, C4, s1.58"},
            {"group": "PUY", "tag": "Puy 0.7, N4.6, b0.902, C4, s1.72"},
        ],
        [
            {"group": "Hik", "tag": "Hik TL, N27.9, b1.241, C4, s0.42"},
            {"group": "PUY", "tag": "Puy 0.7, N4.6, b0.902, C4, s0.28"},
        ],
        [
            {"group": "Hik", "tag": "Hik TL, N27.9, b1.241, C4, s1"},
            {"group": "PUY", "tag": "Puy 0.7, N4.6, b0.902, C4, s1"},
        ],
        [
            {"group": "Hik", "tag": "Hik TL, N27.9, b1.241, C4, s1.58"},
            {"group": "PUY", "tag": "Puy 0.7, N4.6, b0.902, C4, s1.72"},
        ],
    ],
}

logic_tree_permutations = [
    [
        {
            "tag": "Final SLT for TAG Workshop",
            "weight": 1.0,
            "permute": [
                {
                    "group": "Hik",
                    "members": [
                        {
                            "tag": "Hik TL, N16.5, b0.95, C4, s0.42",
                            "weight": 0.147216637218474,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwMzM5MzI=",
                            "bg_id": "RmlsZToxMzA3MzI=",
                        },
                        {
                            "tag": "Hik TL, N16.5, b0.95, C4, s1",
                            "weight": 0.281154911079988,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwMzM5MjI=",
                            "bg_id": "RmlsZToxMzA3MzQ=",
                        },
                        {
                            "tag": "Hik TL, N16.5, b0.95, C4, s1.58",
                            "weight": 0.148371277510384,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwMzM5MjY=",
                            "bg_id": "RmlsZToxMzA3MzY=",
                        },
                        {
                            "tag": "Hik TL, N21.5, b1.097, C4, s0.42",
                            "weight": 0.0887399956599006,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwMzM5Mjc=",
                            "bg_id": "RmlsZToxMzA3Mzg=",
                        },
                        {
                            "tag": "Hik TL, N21.5, b1.097, C4, s1",
                            "weight": 0.169475991711261,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwMzM5MjE=",
                            "bg_id": "RmlsZToxMzA3NDA=",
                        },
                        {
                            "tag": "Hik TL, N21.5, b1.097, C4, s1.58",
                            "weight": 0.0894359956258606,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwMzM5MzQ=",
                            "bg_id": "RmlsZToxMzA3NDI=",
                        },
                        {
                            "tag": "Hik TL, N27.9, b1.241, C4, s0.42",
                            "weight": 0.0192986223768806,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwMzM5MTk=",
                            "bg_id": "RmlsZToxMzA3NDQ=",
                        },
                        {
                            "tag": "Hik TL, N27.9, b1.241, C4, s1",
                            "weight": 0.0368565846962387,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwMzM5MTI=",
                            "bg_id": "RmlsZToxMzA3NDY=",
                        },
                        {
                            "tag": "Hik TL, N27.9, b1.241, C4, s1.58",
                            "weight": 0.019449984121013,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwMzM5MTM=",
                            "bg_id": "RmlsZToxMzA3NDg=",
                        },
                    ],
                },
                {
                    "group": "PUY",
                    "members": [
                        {
                            "tag": "Puy 0.7, N4.6, b0.902, C4, s0.28",
                            "weight": 0.21,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExODcxNw==",
                            "bg_id": "RmlsZToxMzA3NTM=",
                        },
                        {
                            "tag": "Puy 0.7, N4.6, b0.902, C4, s1",
                            "weight": 0.52,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExODcyMw==",
                            "bg_id": "RmlsZToxMzA3NTQ=",
                        },
                        {
                            "tag": "Puy 0.7, N4.6, b0.902, C4, s1.72",
                            "weight": 0.27,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExODcyOQ==",
                            "bg_id": "RmlsZToxMzA3NTU=",
                        },
                    ],
                },
                {
                    "group": "CRU",
                    "members": [
                        {
                            "tag": "geodetic, TI, N2.7, b0.823 C4.2 s0.66",
                            "weight": 0.0168335471189857,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg5OA==",
                            "bg_id": "RmlsZToxMzA3MDc=",
                        },
                        {
                            "tag": "geodetic, TI, N2.7, b0.823 C4.2 s1",
                            "weight": 0.0408928149352719,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkxMQ==",
                            "bg_id": "RmlsZToxMzA3MTA=",
                        },
                        {
                            "tag": "geodetic, TI, N2.7, b0.823 C4.2 s1.41",
                            "weight": 0.0216771620919014,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkwNA==",
                            "bg_id": "RmlsZToxMzA3MTM=",
                        },
                        {
                            "tag": "geodetic, TI, N3.4, b0.959 C4.2 s0.66",
                            "weight": 0.0282427120823285,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkxNw==",
                            "bg_id": "RmlsZToxMzA3MTY=",
                        },
                        {
                            "tag": "geodetic, TI, N3.4, b0.959 C4.2 s1",
                            "weight": 0.0686084751056566,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkwNg==",
                            "bg_id": "RmlsZToxMzA3MTk=",
                        },
                        {
                            "tag": "geodetic, TI, N3.4, b0.959 C4.2 s1.41",
                            "weight": 0.0363691528229985,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkwOA==",
                            "bg_id": "RmlsZToxMzA3MjI=",
                        },
                        {
                            "tag": "geodetic, TI, N4.6, b1.089 C4.2 s0.66",
                            "weight": 0.00792374079868575,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkxMw==",
                            "bg_id": "RmlsZToxMzA3MjU=",
                        },
                        {
                            "tag": "geodetic, TI, N4.6, b1.089 C4.2 s1",
                            "weight": 0.0192487099590715,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkxMg==",
                            "bg_id": "RmlsZToxMzA3Mjg=",
                        },
                        {
                            "tag": "geodetic, TI, N4.6, b1.089 C4.2 s1.41",
                            "weight": 0.0102036850851,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkyNA==",
                            "bg_id": "RmlsZToxMzA3MzE=",
                        },
                        {
                            "tag": "geologic, TI, N2.7, b0.823 C4.2 s0.66",
                            "weight": 0.0236560148670262,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg4Ng==",
                            "bg_id": "RmlsZToxMzA3MDc=",
                        },
                        {
                            "tag": "geologic, TI, N2.7, b0.823 C4.2 s1",
                            "weight": 0.0574662625307477,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg5Nw==",
                            "bg_id": "RmlsZToxMzA3MTA=",
                        },
                        {
                            "tag": "geologic, TI, N2.7, b0.823 C4.2 s1.41",
                            "weight": 0.0304626983900857,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg3NA==",
                            "bg_id": "RmlsZToxMzA3MTM=",
                        },
                        {
                            "tag": "geologic, TI, N3.4, b0.959 C4.2 s0.66",
                            "weight": 0.0271169544446554,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg5Mw==",
                            "bg_id": "RmlsZToxMzA3MTY=",
                        },
                        {
                            "tag": "geologic, TI, N3.4, b0.959 C4.2 s1",
                            "weight": 0.0658737336745166,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg4OQ==",
                            "bg_id": "RmlsZToxMzA3MTk=",
                        },
                        {
                            "tag": "geologic, TI, N3.4, b0.959 C4.2 s1.41",
                            "weight": 0.0349194743556176,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg3MQ==",
                            "bg_id": "RmlsZToxMzA3MjI=",
                        },
                        {
                            "tag": "geologic, TI, N4.6, b1.089 C4.2 s0.66",
                            "weight": 0.00222703068831837,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg4MQ==",
                            "bg_id": "RmlsZToxMzA3MjU=",
                        },
                        {
                            "tag": "geologic, TI, N4.6, b1.089 C4.2 s1",
                            "weight": 0.00541000379473566,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg5Mg==",
                            "bg_id": "RmlsZToxMzA3Mjg=",
                        },
                        {
                            "tag": "geologic, TI, N4.6, b1.089 C4.2 s1.41",
                            "weight": 0.00286782725429677,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg4Mg==",
                            "bg_id": "RmlsZToxMzA3MzE=",
                        },
                        {
                            "tag": "geodetic, TD, N2.7, b0.823 C4.2 s0.66",
                            "weight": 0.0168335471189857,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk2NQ==",
                            "bg_id": "RmlsZToxMzA3MDc=",
                        },
                        {
                            "tag": "geodetic, TD, N2.7, b0.823 C4.2 s1",
                            "weight": 0.0408928149352719,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk2Mw==",
                            "bg_id": "RmlsZToxMzA3MTA=",
                        },
                        {
                            "tag": "geodetic, TD, N2.7, b0.823 C4.2 s1.41",
                            "weight": 0.0216771620919014,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk1Ng==",
                            "bg_id": "RmlsZToxMzA3MTM=",
                        },
                        {
                            "tag": "geodetic, TD, N3.4, b0.959 C4.2 s0.66",
                            "weight": 0.0282427120823285,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk1Mw==",
                            "bg_id": "RmlsZToxMzA3MTY=",
                        },
                        {
                            "tag": "geodetic, TD, N3.4, b0.959 C4.2 s1",
                            "weight": 0.0686084751056566,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk2Nw==",
                            "bg_id": "RmlsZToxMzA3MTk=",
                        },
                        {
                            "tag": "geodetic, TD, N3.4, b0.959 C4.2 s1.41",
                            "weight": 0.0363691528229985,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk1NA==",
                            "bg_id": "RmlsZToxMzA3MjI=",
                        },
                        {
                            "tag": "geodetic, TD, N4.6, b1.089 C4.2 s0.66",
                            "weight": 0.00792374079868575,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk3Mw==",
                            "bg_id": "RmlsZToxMzA3MjU=",
                        },
                        {
                            "tag": "geodetic, TD, N4.6, b1.089 C4.2 s1",
                            "weight": 0.0192487099590715,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk1NQ==",
                            "bg_id": "RmlsZToxMzA3Mjg=",
                        },
                        {
                            "tag": "geodetic, TD, N4.6, b1.089 C4.2 s1.41",
                            "weight": 0.0102036850851,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk3NA==",
                            "bg_id": "RmlsZToxMzA3MzE=",
                        },
                        {
                            "tag": "geologic, TD, N2.7, b0.823 C4.2 s0.66",
                            "weight": 0.0236560148670263,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkzNg==",
                            "bg_id": "RmlsZToxMzA3MDc=",
                        },
                        {
                            "tag": "geologic, TD, N2.7, b0.823 C4.2 s1",
                            "weight": 0.0574662625307477,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkyNw==",
                            "bg_id": "RmlsZToxMzA3MTA=",
                        },
                        {
                            "tag": "geologic, TD, N2.7, b0.823 C4.2 s1.41",
                            "weight": 0.0304626983900857,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkzMg==",
                            "bg_id": "RmlsZToxMzA3MTM=",
                        },
                        {
                            "tag": "geologic, TD, N3.4, b0.959 C4.2 s0.66",
                            "weight": 0.0271169544446554,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk1MQ==",
                            "bg_id": "RmlsZToxMzA3MTY=",
                        },
                        {
                            "tag": "geologic, TD, N3.4, b0.959 C4.2 s1",
                            "weight": 0.0658737336745166,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk0NA==",
                            "bg_id": "RmlsZToxMzA3MTk=",
                        },
                        {
                            "tag": "geologic, TD, N3.4, b0.959 C4.2 s1.41",
                            "weight": 0.0349194743556176,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk0OA==",
                            "bg_id": "RmlsZToxMzA3MjI=",
                        },
                        {
                            "tag": "geologic, TD, N4.6, b1.089 C4.2 s0.66",
                            "weight": 0.00222703068831837,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkzNA==",
                            "bg_id": "RmlsZToxMzA3MjU=",
                        },
                        {
                            "tag": "geologic, TD, N4.6, b1.089 C4.2 s1",
                            "weight": 0.00541000379473566,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkzMQ==",
                            "bg_id": "RmlsZToxMzA3Mjg=",
                        },
                        {
                            "tag": "geologic, TD, N4.6, b1.089 C4.2 s1.41",
                            "weight": 0.00286782725429677,
                            "inv_id": "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk0Mw==",
                            "bg_id": "RmlsZToxMzA3MzE=",
                        },
                    ],
                },
                {
                    "group": "SLAB",
                    "members": [
                        {"tag": "slab-uniform-1depth-rates", "weight": 1.0, "inv_id": "", "bg_id": "RmlsZToxMjEwMzM="}
                    ],
                },
            ],
        }
    ]
]


gtdata = {
    "data": {
        "node1": {
            "id": "R2VuZXJhbFRhc2s6MTI5MjQz",
            "children": {
                "total_count": 49,
                "edges": [
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TI, N2.7, b0.823 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg3NA==', 'bg_id': 'RmlsZToxMzA3MTM=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDg0"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TD, N4.6, b1.089 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk1NQ==', 'bg_id': 'RmlsZToxMzA3Mjg=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDUx"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Puy 0.7, N4.6, b0.902, C4, s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExODcyMw==', 'bg_id': 'RmlsZToxMzA3NTQ=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5MzQ0"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TI, N2.7, b0.823 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkxMQ==', 'bg_id': 'RmlsZToxMzA3MTA=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDcy"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TI, N4.6, b1.089 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg4MQ==', 'bg_id': 'RmlsZToxMzA3MjU=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDY5"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Puy 0.7, N4.6, b0.902, C4, s1.72', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExODcyOQ==', 'bg_id': 'RmlsZToxMzA3NTU=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5MzQy"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Hik TL, N27.9, b1.241, C4, s1.58', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExNDQwOQ==', 'bg_id': 'RmlsZToxMzA3NDg=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5Mzgz"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TI, N4.6, b1.089 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkxMg==', 'bg_id': 'RmlsZToxMzA3Mjg=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDUw"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TD, N2.7, b0.823 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk2Mw==', 'bg_id': 'RmlsZToxMzA3MTA=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDky"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TD, N3.4, b0.959 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk1Mw==', 'bg_id': 'RmlsZToxMzA3MTY=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDQ5"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TI, N2.7, b0.823 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkwNA==', 'bg_id': 'RmlsZToxMzA3MTM=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDc0"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TI, N3.4, b0.959 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg4OQ==', 'bg_id': 'RmlsZToxMzA3MTk=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDU4"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TI, N2.7, b0.823 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg5Nw==', 'bg_id': 'RmlsZToxMzA3MTA=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDg5"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TI, N3.4, b0.959 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg5Mw==', 'bg_id': 'RmlsZToxMzA3MTY=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDc2"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TD, N3.4, b0.959 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk1NA==', 'bg_id': 'RmlsZToxMzA3MjI=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDY0"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Puy 0.7, N4.6, b0.902, C4, s0.28', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExODcxNw==', 'bg_id': 'RmlsZToxMzA3NTM=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5MzQz"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Hik TL, N27.9, b1.241, C4, s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExNDQwNg==', 'bg_id': 'RmlsZToxMzA3NDY=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5Mzgy"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Hik TL, N16.5, b0.95, C4, s1.58', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExNDQyOQ==', 'bg_id': 'RmlsZToxMzA3MzY=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5Mzk1"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TI, N3.4, b0.959 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkxNw==', 'bg_id': 'RmlsZToxMzA3MTY=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDYz"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Hik TL, N16.5, b0.95, C4, s0.42', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExNDQyOA==', 'bg_id': 'RmlsZToxMzA3MzI=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5Mzk2"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TD, N3.4, b0.959 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk2Nw==', 'bg_id': 'RmlsZToxMzA3MTk=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDgz"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TI, N3.4, b0.959 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkwOA==', 'bg_id': 'RmlsZToxMzA3MjI=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDY2"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TD, N4.6, b1.089 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk3Mw==', 'bg_id': 'RmlsZToxMzA3MjU=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDc1"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TD, N2.7, b0.823 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk2NQ==', 'bg_id': 'RmlsZToxMzA3MDc=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDcw"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TI, N3.4, b0.959 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkwNg==', 'bg_id': 'RmlsZToxMzA3MTk=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDkz"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TI, N4.6, b1.089 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkxMw==', 'bg_id': 'RmlsZToxMzA3MjU=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDYx"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TI, N4.6, b1.089 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg5Mg==', 'bg_id': 'RmlsZToxMzA3Mjg=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDcx"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TI, N2.7, b0.823 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg4Ng==', 'bg_id': 'RmlsZToxMzA3MDc=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDc4"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Hik TL, N21.5, b1.097, C4, s1.58', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExNDQxNg==', 'bg_id': 'RmlsZToxMzA3NDI=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5Mzg3"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TI, N4.6, b1.089 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkyNA==', 'bg_id': 'RmlsZToxMzA3MzE=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDYy"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Hik TL, N21.5, b1.097, C4, s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExNDQxMw==', 'bg_id': 'RmlsZToxMzA3NDA=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5Mzg1"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Hik TL, N21.5, b1.097, C4, s0.42', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExNDQyNA==', 'bg_id': 'RmlsZToxMzA3Mzg=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5Mzg2"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TI, N4.6, b1.089 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg4Mg==', 'bg_id': 'RmlsZToxMzA3MzE=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDY3"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TD, N2.7, b0.823 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk1Ng==', 'bg_id': 'RmlsZToxMzA3MTM=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDY1"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TI, N2.7, b0.823 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg5OA==', 'bg_id': 'RmlsZToxMzA3MDc=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDY4"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TI, N3.4, b0.959 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDg3MQ==', 'bg_id': 'RmlsZToxMzA3MjI=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDcz"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Hik TL, N16.5, b0.95, C4, s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExNDQxNw==', 'bg_id': 'RmlsZToxMzA3MzQ=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5Mzk0"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'Hik TL, N27.9, b1.241, C4, s0.42', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjExNDQxNA==', 'bg_id': 'RmlsZToxMzA3NDQ=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5Mzg0"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'slab-uniform-1depth-rates', 'inv_id': '', 'bg_id': 'RmlsZToxMjEwMzM=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDE4"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TD, N3.4, b0.959 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk0NA==', 'bg_id': 'RmlsZToxMzA3MTk=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDc3"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TD, N2.7, b0.823 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkyNw==', 'bg_id': 'RmlsZToxMzA3MTA=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDQ4"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TD, N4.6, b1.089 C4.2 s1', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkzMQ==', 'bg_id': 'RmlsZToxMzA3Mjg=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDk5"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TD, N4.6, b1.089 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk3NA==', 'bg_id': 'RmlsZToxMzA3MzE=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDk1"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TD, N2.7, b0.823 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkzMg==', 'bg_id': 'RmlsZToxMzA3MTM=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NTAy"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TD, N3.4, b0.959 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk1MQ==', 'bg_id': 'RmlsZToxMzA3MTY=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDk3"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TD, N3.4, b0.959 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk0OA==', 'bg_id': 'RmlsZToxMzA3MjI=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDk0"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TD, N4.6, b1.089 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDk0Mw==', 'bg_id': 'RmlsZToxMzA3MzE=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDk4"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TD, N2.7, b0.823 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkzNg==', 'bg_id': 'RmlsZToxMzA3MDc=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NTAw"},
                            }
                        }
                    },
                    {
                        "node": {
                            "child": {
                                "arguments": [
                                    {"k": "config_archive_id", "v": "RmlsZToxMzY0MDY="},
                                    {"k": "model_type", "v": "COMPOSITE"},
                                    {
                                        "k": "logic_tree_permutations",
                                        "v": "[{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geologic, TD, N4.6, b1.089 C4.2 s0.66', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyMDkzNA==', 'bg_id': 'RmlsZToxMzA3MjU=', 'weight': 1.0}]}]}]",
                                    },
                                    {
                                        "k": "intensity_spec",
                                        "v": "{'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}",
                                    },
                                    {"k": "vs30", "v": "400"},
                                    {"k": "location_code", "v": "GRD_NZ_0_1_NZ34_BA"},
                                    {"k": "disagg_conf", "v": "{'enabled': False, 'config': {}}"},
                                    {"k": "rupture_mesh_spacing", "v": "5"},
                                    {"k": "ps_grid_spacing", "v": "30"},
                                    {"k": "split_source_branches", "v": "False"},
                                ],
                                "result": "SUCCESS",
                                "hazard_solution": {"id": "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTI5NDk2"},
                            }
                        }
                    },
                ],
            },
        }
    }
}
