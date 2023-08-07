//#pragma once
//
//#include <string>
//
// namespace flounder {
// class Annotation
//{
// public:
//    enum Type
//    {
//        Loop,
//        Branch,
//        Prefetch,
//        ScanPrefetch,
//    };
//
//    constexpr Annotation(const Type type) noexcept : _type(type) {}
//    constexpr Annotation(Annotation &&) noexcept = default;
//    Annotation(const Annotation &) = default;
//    virtual ~Annotation() noexcept = default;
//
//    Annotation &operator=(Annotation &&) noexcept = default;
//    Annotation &operator=(const Annotation &) = default;
//
//    [[nodiscard]] Type type() const noexcept { return _type; }
//    [[nodiscard]] virtual std::string to_string() const = 0;
//
// private:
//    Type _type;
//};
//} // namespace flounder