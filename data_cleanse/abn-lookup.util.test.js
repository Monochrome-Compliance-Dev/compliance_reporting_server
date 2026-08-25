const {
  classifyGovernmentEntityType,
  lookupAbnByNumber,
  parseExactAbnLookupResponse,
} = require("./abn-lookup.util");

describe("ABR exact ABN lookup parsing", () => {
  test("classifies government entity types from ABR reference codes", () => {
    expect(classifyGovernmentEntityType("CGE")).toBe(true);
    expect(classifyGovernmentEntityType("LOC")).toBe(true);
    expect(classifyGovernmentEntityType("PUB")).toBe(false);
    expect(classifyGovernmentEntityType("IND")).toBe(false);
    expect(classifyGovernmentEntityType("CCZ")).toBe(false);
  });

  test("parses a government ABN response from ABRSearchByABN", () => {
    const xml = `
      <ABRPayloadSearchResults xmlns="http://abr.business.gov.au/ABRXMLSearch/">
        <response>
          <businessEntity>
            <ABN>
              <identifierValue>51 835 430 479</identifierValue>
              <isCurrentIndicator>Y</isCurrentIndicator>
            </ABN>
            <entityStatus>
              <entityStatusCode>Active</entityStatusCode>
            </entityStatus>
            <entityType>
              <entityTypeCode>CGE</entityTypeCode>
              <entityDescription>Commonwealth Government Entity</entityDescription>
            </entityType>
            <mainName>
              <organisationName>Department Example</organisationName>
            </mainName>
          </businessEntity>
        </response>
      </ABRPayloadSearchResults>
    `;

    expect(parseExactAbnLookupResponse(xml)).toMatchObject({
      found: true,
      abn: "51835430479",
      isCurrentAbn: true,
      entityTypeCode: "CGE",
      entityTypeDescription: "Commonwealth Government Entity",
      name: "Department Example",
      isGovernmentEntity: true,
    });
  });

  test("returns not found for ABR exception payloads", () => {
    const xml = `
      <ABRPayloadSearchResults xmlns="http://abr.business.gov.au/ABRXMLSearch/">
        <response>
          <exception>
            <exceptionDescription>ABN not found</exceptionDescription>
          </exception>
        </response>
      </ABRPayloadSearchResults>
    `;

    expect(parseExactAbnLookupResponse(xml)).toEqual({
      found: false,
      exception: "ABN not found",
    });
  });

  test("classifies the ASIC CGE result as government and Boral PUB as non-government", () => {
    const asicXml = `
      <businessEntity>
        <ABN><identifierValue>86768265615</identifierValue><isCurrentIndicator>Y</isCurrentIndicator></ABN>
        <entityStatus><entityStatusCode>Active</entityStatusCode></entityStatus>
        <entityType><entityTypeCode>CGE</entityTypeCode><entityDescription>Commonwealth Government Entity</entityDescription></entityType>
        <mainName><organisationName>AUSTRALIAN SECURITIES &amp; INVESTMENTS COMMISSION</organisationName></mainName>
      </businessEntity>
    `;
    const boralXml = `
      <businessEntity>
        <ABN><identifierValue>62008528523</identifierValue><isCurrentIndicator>Y</isCurrentIndicator></ABN>
        <entityStatus><entityStatusCode>Active</entityStatusCode></entityStatus>
        <entityType><entityTypeCode>PUB</entityTypeCode><entityDescription>Australian Public Company</entityDescription></entityType>
        <mainName><organisationName>BORAL CEMENT LIMITED</organisationName></mainName>
      </businessEntity>
    `;

    expect(parseExactAbnLookupResponse(asicXml)).toMatchObject({
      abn: "86768265615",
      isGovernmentEntity: true,
    });
    expect(parseExactAbnLookupResponse(boralXml)).toMatchObject({
      abn: "62008528523",
      isGovernmentEntity: false,
    });
  });

  test("reads ABR_GUID from the centrally loaded process environment", async () => {
    const originalFetch = global.fetch;
    const originalGuid = process.env.ABR_GUID;
    process.env.ABR_GUID = "runtime-guid";
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      text: jest.fn().mockResolvedValue(`
        <businessEntity>
          <ABN><identifierValue>86768265615</identifierValue><isCurrentIndicator>Y</isCurrentIndicator></ABN>
          <entityStatus><entityStatusCode>Active</entityStatusCode></entityStatus>
          <entityType><entityTypeCode>CGE</entityTypeCode></entityType>
        </businessEntity>
      `),
    });

    try {
      await lookupAbnByNumber("86 768 265 615");
      expect(global.fetch).toHaveBeenCalledWith(
        expect.stringContaining("authenticationGuid=runtime-guid"),
      );
    } finally {
      global.fetch = originalFetch;
      if (originalGuid === undefined) {
        delete process.env.ABR_GUID;
      } else {
        process.env.ABR_GUID = originalGuid;
      }
    }
  });

  test("rejects malformed ABNs before making an ABR request", async () => {
    await expect(lookupAbnByNumber("1234")).rejects.toThrow(
      "A valid 11-digit ABN is required",
    );
  });
});
