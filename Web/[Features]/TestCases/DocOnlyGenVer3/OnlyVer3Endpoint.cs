﻿namespace Web._Features_.TestCases.DocOnlyGenVer3;

public class OnlyVer3Endpoint : FastEndpoints.EndpointWithoutRequest
{
    public override void Configure()
    {
        AllowAnonymous();
        Get("OnlyVer3");
        Version(3, 4);
    }


    public async override Task HandleAsync(CancellationToken ct) => await SendOkAsync(ct);
}