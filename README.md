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

## FMR Integration

### Timeline
- **Current Focus**: Factory Qualification Testing (FQT)
- **FMR Kick-off**: December 5th, 2023
- **UAT**: Following FMR kick-off

### Status Overview
- [x] Planning Phase
  - [x] Feature parity assessment
  - [x] Client impact analysis
  - [x] Migration timeline established
- [ ] Implementation Phase
  - [x] Core functionality migration
  - [x] Data format adaptation
  - [ ] FQT Sample Preparation
    - Focus on small, diverse sample set
    - Cover key compliance conditions
    - Validate analysis accuracy
- [ ] Testing Phase
  - [ ] FQT (Current Priority)
    - Targeted completion: Before Dec 5th
    - Focus on compliance condition coverage
    - Document test findings
  - [ ] UAT Environment Setup
    - Scheduled after FMR kick-off
    - Will focus on source system validation
    - Customer verification of analysis accuracy
  - [ ] Performance validation
- [ ] Production Phase
  - [ ] Client readiness verification
  - [ ] Production environment preparation
  - [ ] Cutover planning

### Testing Strategy
1. **FQT (Current Phase)**
   - Prepare concentrated sample set showcasing various compliance conditions
   - Validate analysis accuracy against known scenarios
   - Document and address any discovered issues
   - Goal: Complete comprehensive testing before FMR kick-off

2. **UAT (Post Dec 5th)**
   - Customer-driven validation
   - Focus on comparing analysis results with source systems
   - Verify accuracy across different compliance scenarios
   - Gather and incorporate customer feedback

### Testing Progress Status
| Component | Unit Testing | FQT | UAT | Production | Notes |
|-----------|--------------|-----|-----|------------|-------|
| Core Analysis | 🟡 | ⚪ | ⚪ | ⚪ | Unit testing in progress |
| Data Extraction | 🟡 | ⚪ | ⚪ | ⚪ | Completing final unit tests |
| Compliance Rules | 🟢 | ⚪ | ⚪ | ⚪ | Ready for FQT |
| Integration Layer | 🟡 | ⚪ | ⚪ | ⚪ | API tests in progress |

Legend:
- ⚪ Not Started
- 🟡 In Progress
- 🟢 Completed
- 🔴 Blocked


## Legacy Support

While V2 is the current recommended version, the extractr codebase is maintained for reference and compatibility testing. This ensures a smooth transition for applications that may still depend on the original data format.


## Issue Tracking

This repository uses GitHub Issues as a supplemental tracking system specifically for long-lived technical issues. It is not intended to replace the development team's primary ticket tracking system.

### When to Use GitHub Issues

Use GitHub Issues for:
- Long-term technical challenges that span multiple releases
- Architectural decisions that require ongoing discussion
- Technical debt items that need persistent tracking
- Cross-cutting concerns affecting multiple components

### When Not to Use GitHub Issues

Do not use GitHub Issues for:
- Regular sprint tasks and features
- Bug fixes that can be resolved in current sprint
- Team dependencies and assignments
- Standard development workflow items

The goal is to maintain clear separation between day-to-day development tracking and persistent technical challenges that require extended attention and documentation.

