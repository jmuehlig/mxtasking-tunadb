#include "comparator.h"

using namespace flounder;

Comparator &IsEquals::invert() noexcept
{
    return *new (this) IsNotEquals(left(), right(), Comparator::is_likely());
}

Comparator &IsNotEquals::invert() noexcept
{
    return *new (this) IsEquals(left(), right(), Comparator::is_likely());
}

Comparator &IsLower::invert() noexcept
{
    return *new (this) IsGreaterEquals(left(), right(), Comparator::is_likely());
}

Comparator &IsLowerEquals::invert() noexcept
{
    return *new (this) IsGreater(left(), right(), Comparator::is_likely());
}

Comparator &IsGreater::invert() noexcept
{
    return *new (this) IsLowerEquals(left(), right(), Comparator::is_likely());
}

Comparator &IsGreaterEquals::invert() noexcept
{
    return *new (this) IsLower(left(), right(), Comparator::is_likely());
}