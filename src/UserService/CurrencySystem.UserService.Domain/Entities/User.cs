namespace CurrencySystem.UserService.Domain.Entities;

/// <summary>
/// Сущность пользователя
/// </summary>
public class User
{
    public Guid Id { get; private set; }
    public string Name { get; private set; } = string.Empty;
    public string PasswordHash { get; private set; } = string.Empty;
    public List<string> FavoriteCurrencies { get; private set; } = new();
    public DateTime CreatedAt { get; private set; }

    // EF Core constructor
    private User() { }

    public User(Guid id, string name, string passwordHash, List<string> favoriteCurrencies)
    {
        Id = id;
        Name = name;
        PasswordHash = passwordHash;
        FavoriteCurrencies = favoriteCurrencies ?? new List<string>();
        CreatedAt = DateTime.UtcNow;
    }

    public void SetFavoriteCurrencies(List<string> currencies)
    {
        FavoriteCurrencies = currencies ?? new List<string>();
    }
}