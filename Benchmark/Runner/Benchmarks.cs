﻿using ApiExpress.TestClient;
using BenchmarkDotNet.Attributes;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Runner
{
    [MemoryDiagnoser]
    public class Benchmarks
    {
        public static HttpClient ApiExpressClient { get; } = new WebApplicationFactory<ApiExpressBench.Program>().CreateClient();
        public static HttpClient ControllerClient { get; } = new WebApplicationFactory<MapControllers.Program>().CreateClient();
        public static HttpClient MvcClient { get; } = new WebApplicationFactory<MvcControllers.Program>().CreateClient();

        [Benchmark(Baseline = true)]
        public async Task ApiExpress()
        {
            await ApiExpressClient.PostAsync<ApiExpressBench.Request, ApiExpressBench.Response>(

                "/benchmark/ok/123", new()
                {
                    FirstName = "xxc",
                    LastName = "yyy",
                    Age = 23,
                    PhoneNumbers = new[] {
                        "1111111111",
                        "2222222222",
                        "3333333333",
                        "4444444444",
                        "5555555555"
                    }
                });
        }

        [Benchmark]
        public async Task AspNetCoreController()
        {
            await ApiExpressClient.PostAsync<MapControllers.Request, MapControllers.Response>(

                "/benchmark/ok/123", new()
                {
                    FirstName = "xxc",
                    LastName = "yyy",
                    Age = 23,
                    PhoneNumbers = new[] {
                        "1111111111",
                        "2222222222",
                        "3333333333",
                        "4444444444",
                        "5555555555"
                    }
                });
        }

        [Benchmark]
        public async Task AspNetCoreMVC()
        {
            await ApiExpressClient.PostAsync<MvcControllers.Request, MvcControllers.Response>(

                "/Home/Index/123", new()
                {
                    FirstName = "xxc",
                    LastName = "yyy",
                    Age = 23,
                    PhoneNumbers = new[] {
                        "1111111111",
                        "2222222222",
                        "3333333333",
                        "4444444444",
                        "5555555555"
                    }
                });
        }
    }
}
