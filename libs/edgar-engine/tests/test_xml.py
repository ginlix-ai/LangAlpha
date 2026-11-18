from edgar_engine import _xml


def test_text_is_read_as_filed():
    el = _xml.element(b'<x:root xmlns:x="u"><x:a><![CDATA[keep <fmt:name> and a:b="c"]]></x:a></x:root>', "root")
    assert _xml.text(el, "a") == 'keep <fmt:name> and a:b="c"'


def test_mismatched_prefixes_are_not_well_formed():
    assert _xml.element(b'<root xmlns:a="u"><a:x>1</b:x></root>', "root") is None


def test_a_namespace_fault_alone_is_read_by_local_name():
    assert _xml.text(_xml.element(b"<a:root><a:x>1</a:x></a:root>", "root"), "x") == "1"
    assert _xml.text(_xml.element(b'<root xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance "><x>1</x></root>',
                                  "root"), "x") == "1"


def test_namespace_repair_leaves_cdata_and_comments_as_filed():
    el = _xml.element(b'<a:root><a:x><![CDATA[keep <fmt:name> and a:b="c"]]></a:x><!-- <b:y/> --></a:root>', "root")
    assert _xml.text(el, "x") == 'keep <fmt:name> and a:b="c"'


def test_a_closing_tag_inside_cdata_or_a_comment_does_not_end_the_document():
    el = _xml.element(b"<root><r><![CDATA[literal </root>]]></r><!-- </root> --><x>1</x></root>", "root")
    assert _xml.text(el, "x") == "1"


def test_a_self_closing_root_is_an_empty_document():
    el = _xml.element(b'<XML><ns1:informationTable xmlns:ns1="u"/></XML>', "informationTable")
    assert el is not None and el.tag == "informationTable" and len(el) == 0


def test_a_date_off_the_calendar_reads_as_none():
    assert _xml.iso_date("06-30-2024") == "2024-06-30" and _xml.iso_date("2024-06-30-05:00") == "2024-06-30"
    assert _xml.iso_date("06-31-2024") is None and _xml.iso_date("2024-02-30") is None


def test_a_document_is_decoded_on_its_own():
    raw = "<root><n>Soci\u00e9t\u00e9 G\u00e9n\u00e9rale</n></root>".encode() + b"<other>\xe9</other>"
    assert _xml.text(_xml.element(raw, "root"), "n") == "Soci\u00e9t\u00e9 G\u00e9n\u00e9rale"
