using System;
using System.Collections.Generic;

namespace CurrencySystem.Contracts.Events;

/// <summary>
/// Событие публикуется в Kafka при обновлении курсов валют из CBR
/// Топик: currency.rates.updated
/// </summary>
public record CurrencyRatesUpdatedEvent(
    Guid EventId,
    DateTime OccurredAt,
    IReadOnlyList<CurrencyRateItem> Rates,
    string Source = "cbr.ru"
);

/// <summary>
/// Отдельный курс валюты
/// </summary>
public record CurrencyRateItem(
    string CurrencyCode,    // ISO код: USD, EUR, CNY
    string CurrencyName,    // Название: Доллар, Евро, Юань
    decimal RateToRub,      // Курс к рублю
    decimal Nominal         // Номинал (обычно 1)
);