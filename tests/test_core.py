from miaproc.eddy import load_stage1, postproc, HessefluxConfig

def test_imports():
    assert load_stage1 is not None
    assert postproc is not None
    assert HessefluxConfig is not None