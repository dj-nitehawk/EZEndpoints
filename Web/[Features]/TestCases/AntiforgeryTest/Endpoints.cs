﻿
using Microsoft.AspNetCore.Antiforgery;

namespace TestCases.AntiforgeryTest;
public static class Endpoints
{
    public const string Get = "api/antiforgery/get";
    public const string Post = "api/antiforgery/post";
}

public class Req
{
    public IFormFile File { get; set; }
}
public class Rsp
{

}


public class AntUiEndpoint : Endpoint<EmptyObject>
{
    private readonly IAntiforgery _antiforgery;
    private readonly IHttpContextAccessor _httpContextAccessor;
    public AntUiEndpoint(IAntiforgery antiforgery, IHttpContextAccessor httpContextAccessor)
    {
        _antiforgery = antiforgery;
        _httpContextAccessor = httpContextAccessor;
    }

    public override void Configure()
    {
        Get("ant-ui");
        Tags("antiforgery");
        AllowAnonymous();
    }


    public override Task HandleAsync(EmptyObject req, CancellationToken ct)
    {
        var token = _antiforgery.GetAndStoreTokens(_httpContextAccessor.HttpContext!);
        var html = $"""
              <html>
                <body>
                 <h3>Upload a image test</h3>
                  <form name="form1" action="/api/ant" method="post" enctype="multipart/form-data">
                    <input name="{token.FormFieldName}" type="hidden" value="{token.RequestToken}"/>
                    <input type="file" name="file" placeholder="Upload an image..." accept=".jpg,.png" />
                    <input type="submit" />
                  </form> 
                </body>
              </html>
            """;

        SendStringAsync(html, 200, "text/html");
        return Task.CompletedTask;
    }
}



public class AntEndpoint : Endpoint<Req, string>
{
    public override void Configure()
    {
        Post("ant");
        Tags("antiforgery");
        AllowAnonymous();


        //test antiforgery
        EnlableAntiforgery();

        AllowFileUploads();
    }

    public override Task<string> ExecuteAsync(Req req, CancellationToken ct)
    {





        return Task.FromResult("antiforgery success");
    }
}