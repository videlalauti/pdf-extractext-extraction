from fastapi.testclient import TestClient
from main import app
from unittest.mock import AsyncMock, MagicMock, patch

client = TestClient(app)

PDF = b"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 5 0 R >>
endobj
5 0 obj
<< /Length 44 >>
stream
BT /F1 12 Tf 100 700 Td (Hello World) Tj ET
endstream
endobj
xref
0 4
0000000000 65535 f 
0000000009 00000 n 
0000000115 00000 n 
0000000266 00000 n 
trailer
<< /Size 4 /Root 1 0 R >>
startxref
0000
%%EOF"""


def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["service"] == "extraction-service"


def test_extract_valid_pdf_returns_text():
    with patch(
        "httpx.AsyncClient.post",
        new=AsyncMock(
            return_value=MagicMock(
                status_code=201,
                json=lambda: {"id": "doc-1", "content": "", "checksum": "abc"},
                raise_for_status=lambda: None,
            )
        ),
    ):
        response = client.post("/extract", files={"file": ("doc.pdf", PDF, "application/pdf")})

    assert response.status_code == 200
    assert isinstance(response.json()["text"], str)


def test_extract_non_pdf_returns_400():
    response = client.post("/extract", files={"file": ("doc.txt", b"not a pdf", "text/plain")})

    assert response.status_code == 400