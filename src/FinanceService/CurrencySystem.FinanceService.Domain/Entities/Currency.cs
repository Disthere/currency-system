namespace CurrencySystem.FinanceService.Domain.Entities;

/// <summary>
/// Сущность валюты с курсом к рублю
/// </summary>
public class Currency
{
    public Guid Id { get; private set; }
    public string Name { get; private set; } = string.Empty;
    public string Code { get; private set; } = string.Empty;  // ISO: USD, EUR, CNY
    public decimal Rate { get; private set; }                  // Курс к рублю
    public decimal Nominal { get; private set; } = 1;          // Номинал
    public DateTime UpdatedAt { get; private set; }

    // EF Core constructor
    private Currency() { }

    public Currency(Guid id, string name, string code, decimal rate, decimal nominal)
    {
        Id = id;
        Name = name;
        Code = code;
        Rate = rate;
        Nominal = nominal;
        UpdatedAt = DateTime.UtcNow;
    }

    public void UpdateRate(decimal rate, decimal nominal)
    {
        Rate = rate;
        Nominal = nominal;
        UpdatedAt = DateTime.UtcNow;
    }
}