import axios from "axios";
import xml2js from "xml2js";

/**
 * Looks up an ABN by business name using the ABR XML API.
 * @param {string} businessName - The business name to search for.
 * @returns {Promise<{abn: string, name: string, status: string}|null>} The best matching ABN details or null if not found.
 */
export async function lookupABNByName(businessName) {
  const endpoint = "https://abr.business.gov.au/abrxmlsearch/abrxmlsearch.asmx";
  const guid = process.env.ABR_GUID; // ABR API GUID stored in env

  // Build the SOAP request envelope
  const soapEnvelope = `<?xml version="1.0" encoding="utf-8"?>
  <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Body>
      <SearchByName xmlns="http://abr.business.gov.au/ABRXMLSearch/">
        <authenticationGuid>${guid}</authenticationGuid>
        <name>${businessName}</name>
        <minimumScore>80</minimumScore>
        <maximumResults>1</maximumResults>
      </SearchByName>
    </soap:Body>
  </soap:Envelope>`;

  try {
    const response = await axios.post(endpoint, soapEnvelope, {
      headers: {
        "Content-Type": "text/xml; charset=utf-8",
        SOAPAction: "http://abr.business.gov.au/ABRXMLSearch/SearchByName",
      },
    });

    // Parse the XML response
    const parser = new xml2js.Parser({ explicitArray: false });
    const result = await parser.parseStringPromise(response.data);

    // Navigate to the first business match
    const matches =
      result["soap:Envelope"]["soap:Body"]["SearchByNameResponse"][
        "SearchByNameResult"
      ]["response"]["businessEntity201408"];
    if (!matches) {
      return null; // No matches found
    }

    const firstMatch = Array.isArray(matches) ? matches[0] : matches;

    // Return simplified result
    return {
      abn: firstMatch.Abn["identifierValue"],
      name: firstMatch.MainName["organisationName"],
      status: firstMatch.EntityStatus["entityStatusCode"],
    };
  } catch (error) {
    console.error("Error in ABR name lookup:", error);
    throw error;
  }
}
