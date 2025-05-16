from xml.etree.ElementTree import iterparse

input_file = "enwiki-20250501-pages-articles-multistream.xml"
output_file = "g-wiki.xml"

with open(output_file, "w", encoding="utf-8") as out:
    out.write('<?xml version="1.0"?>\n<mediawiki>\n')
    for event, elem in iterparse(input_file, events=("end",)):
        if elem.tag == "page":
            title = elem.find("title")
            if title is not None and title.text.startswith("G"):
                out.write(ElementTree.tostring(elem, encoding="unicode"))
            elem.clear()
    out.write("</mediawiki>\n")
