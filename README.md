# Milton - Multi-Project Repository

This repository serves as the root folder for the evolution of our document processing solution, containing multiple versions and iterations of the product.

## Project Structure

This is a monorepo that houses different versions of our document processing system:

- **V2** (Current) - The latest version of the product, representing a complete redesign and enhancement of the original extractr system.
- **extractr** (Legacy) - The first version of the product, maintained for reference and compatibility testing.

## Version History

### V2 (Current)
The current production version represents a significant evolution from the original extractr system. It maintains the core concepts while introducing improved processing capabilities and enhanced features.

### extractr (Legacy)
The original version of the product contains valuable test samples and scenarios derived from processing hundreds of files across various iterations. This version is maintained for:
- Reference implementation and testing scenarios
- Historical test cases and edge case handling
- Compatibility testing for applications still consuming the original data format
- Fallback support for legacy integrations

## Repository Purpose

This repository structure allows us to:
1. Maintain clear version separation
2. Preserve valuable test cases and scenarios
3. Support gradual migration from extractr to V2
4. Maintain backwards compatibility when needed
5. Document the evolution of the product

## Development

Each version maintains its own documentation, dependencies, and build instructions within its respective directory. Please refer to the specific version's documentation for detailed information about setup and usage.

## Legacy Support

While V2 is the current recommended version, the extractr codebase is maintained for reference and compatibility testing. This ensures a smooth transition for applications that may still depend on the original data format.

---

For specific version documentation, please navigate to the respective version directories:
- `/V2` - Current version
- `/extractr` - Legacy version 